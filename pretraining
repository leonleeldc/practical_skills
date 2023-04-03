class PretrainingDataset(Dataset):
    def __init__(self, data):
        self.data = data

    def __len__(self):
        return len(self.data)

    def __getitem__(self, idx):
        title, taxonomy = self.data[idx]
       
        # Combine title and taxonomy for MLM task
        combined_text = f"{title} [SEP] {taxonomy}"
       
        # Encode the combined text
        input = tokenizer(combined_text, padding='max_length', truncation=True, return_tensors='pt')
       
        # Mask tokens for the MLM task
        inputs = tokenizer.mask_tokens(input)
        return inputs

pretrain_dataset = PretrainingDataset(all_data)

data_collator = DataCollatorForLanguageModeling(tokenizer=tokenizer, mlm=True, mlm_probability=0.15)

model = AutoModelForPreTraining.from_pretrained(model_name)

training_args = TrainingArguments(
    output_dir='./pretrain_results',
    num_train_epochs=3,
    per_device_train_batch_size=16,
    per_device_eval_batch_size=16,
    logging_dir='./pretrain_logs',
)

trainer = Trainer(
    model=model,
    args=training_args,
    train_dataset=pretrain_dataset,
    data_collator=data_collator,
)

trainer.train()

# Save the pretrained model
model.save_pretrained("pretrained_model")