package kvcache

import (
	"context"
	"fmt"
	"os"

	"k8s.io/klog/v2"

	"github.com/neuralmagic/distributed-kv-cache/pkg/tokenization"

	"github.com/redis/go-redis/v9"
)

// Config holds the configuration for the KVCacheIndex.
type Config struct {
	LMCacheEngineConfig
	LMCacheEngineMetadata

	ScoringStrategy KVScoringStrategy
}

// KVCacheIndexer is a concrete implementation of the KVCacheIndex interface.
type KVCacheIndexer struct {
	tokensIndexer   tokenization.Indexer // gets tokens for a prompt
	tokensProcessor TokenProcessor       // turns tokens to kv block keys
	kvBlockIndexer  KVBlockIndexer       // looks up pods for block keys
	kvBlockScorer   KVBlockScorer        // scores pods based on block hits

	tokenizersPool *tokenization.Pool
}

// NewKVCacheIndexer creates a KVCacheIndex given a Config.
func NewKVCacheIndexer(cfg Config) (*KVCacheIndexer, error) {
	scorer, err := NewKVBlockScorer(cfg.ScoringStrategy)
	if err != nil {
		return nil, fmt.Errorf("failed to create KVBlockScorer: %w", err)
	}

	// TODO: move somewhere else
	redisClient := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_HOST"),
		Password: os.Getenv("REDIS_PASSWORD"), // no password set
		DB:       0,                           // use default DB
	})

	_, err = redisClient.Ping(context.Background()).Result()
	if err != nil {
		return nil, fmt.Errorf("could not connect to Redis: %w", err)
	}

	tokensIndexer, err := tokenization.NewLRUTokenStore(&tokenization.LRUStoreConfig{
		BlockSize: tokenization.DefaultBlockSize,
		CacheSize: tokenization.DefaultMaxCacheSize,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create token indexer: %w", err)
	}

	return &KVCacheIndexer{
		tokensIndexer:   tokensIndexer,
		tokensProcessor: NewChunkedTokenDatabase(cfg.LMCacheEngineConfig, cfg.LMCacheEngineMetadata),
		kvBlockIndexer:  NewRedisKVBlockIndexer(redisClient),
		kvBlockScorer:   scorer,
		tokenizersPool:  tokenization.NewTokenizationPool(5, tokensIndexer),
	}, nil
}

// Run starts the indexer.
func (k *KVCacheIndexer) Run(ctx context.Context) {
	k.tokenizersPool.Run(ctx)
}

func (k *KVCacheIndexer) GetPodScores(ctx context.Context, prompt, modelName string) ([]PodScore, error) {
	logger := klog.FromContext(ctx).WithName("kvcache-indexer")
	// 0. add to tokenizers pool
	k.tokenizersPool.AddTask(prompt, modelName)

	// 1. get available tokens of longest prefix
	tokens := k.tokensIndexer.FindLongestContainedTokens(prompt, modelName)
	if len(tokens) == 0 {
		return nil, nil
	}

	// 2. get block keys
	blockKeys := k.tokensProcessor.TokensToKVBlockKeys(tokens, modelName)
	logger.Info("made block keys", "blockKeys", blockKeys)

	// 3. query kvblock indexer for pods
	strBlockKeys, keyToPods, err := k.kvBlockIndexer.GetPodsForKeys(ctx, blockKeys)
	if err != nil {
		return nil, fmt.Errorf("failed to query kvblock indexer: %w", err)
	}
	logger.Info("queried kvblock indexer", "strBlockKeys", strBlockKeys, "keyToPods", keyToPods)

	// 4. score pods
	podScores, err := k.kvBlockScorer.Score(strBlockKeys, keyToPods)
	if err != nil {
		return nil, fmt.Errorf("failed to query kvblock scorer: %w", err)
	}

	return podScores, nil
}
