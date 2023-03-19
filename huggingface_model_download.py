from transformers import AutoModel, AutoTokenizer

model_name = "snunlp/KR-BERT-char16424"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModel.from_pretrained(model_name)