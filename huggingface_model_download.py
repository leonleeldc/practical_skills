from transformers import AutoModel, AutoTokenizer

model_name = "snunlp/KR-BERT-char16424"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModel.from_pretrained(model_name)
# Choose a directory to save the model and tokenizer
save_directory = "path/to/your/save_directory"
# Save the tokenizer
tokenizer.save_pretrained(save_directory)
# Save the model
model.save_pretrained(save_directory)

# Load the tokenizer and model from the save directory
loaded_tokenizer = AutoTokenizer.from_pretrained(save_directory)
loaded_model = AutoModel.from_pretrained(save_directory)
