from normaliser import LLMNormalizer, LLMNormalizerConfig

config = LLMNormalizerConfig()
normalizer = LLMNormalizer(config)

result = normalizer.normalize_item("Caesar Augustus IPA pint", units=2.3)

print(result)