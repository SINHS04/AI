import torch
from torch import nn
from earlystopping import EarlyStopping
from datetime import datetime
from tqdm import tqdm

class Trainer:
    def __init__(
        self, projet_name, model, optimizer, train_data_loader, valid_data_loader, 
        transforms, wandb, device, checkpoint_file_path
    ):
        """
        Required parameter to make Trainer
        """
        self.project_name = projet_name
        self.model = model
        self.optimizer = optimizer
        self.train_data_loader = train_data_loader
        self.valid_data_loader = valid_data_loader
        self.transforms = transforms
        self.wandb = wandb
        self.device = device
        self.checkpoint_file_path = checkpoint_file_path

        self.loss_fn = nn.CrossEntropyLoss()

    def do_train(self):
        """
        Train model with given data.
        Use various generalization methods.
        One epoch with various steps.
        Return training loss and accuracy.
        """
        self.model.train()  # Set model to train

        loss_train = 0.0
        num_corrects_train = 0
        num_trained_samples = 0
        num_trains = 0

        for train_batch in tqdm(self.train_data_loader):  # step
            input_train, target_train = train_batch
            input_train = input_train.to(device=self.device)
            target_train = target_train.to(device=self.device)
            # input_train = self.transforms(input_train)  # Normalize data

            output_train, output_train_aux1, output_train_aux2 = self.model(input_train)
            loss = self.loss_fn(output_train, target_train)
            loss_aux1 = self.loss_fn(output_train_aux1, target_train)
            loss_aux2 = self.loss_fn(output_train_aux2, target_train)
            loss += 0.3 * (loss_aux1 + loss_aux2)
            loss_train += loss.item()   # item() means change value into python scalar

            predicted_train = torch.argmax(output_train, dim=1)
            num_corrects_train += torch.sum(torch.eq(predicted_train, target_train)).item()

            num_trained_samples += len(input_train)
            num_trains += 1

            self.optimizer.zero_grad()
            loss.backward()
            self.optimizer.step()

        train_loss = loss_train / num_trains
        train_accuracy = num_corrects_train / num_trained_samples * 100

        return train_loss, train_accuracy

        
    def do_valid(self):
        """
        Evaluate model with given data.
        Use trained model to evaluate.
        Return validation loss and accuracy.
        """
        self.model.eval()   # Set model to test, which means there aren't any change in model parameter

        loss_valid = 0.0
        num_corrects_valid = 0.0
        num_valid_samples = 0.0
        num_valids = 0

        with torch.no_grad():   # No update
            for valid_batch in tqdm(self.valid_data_loader):
                input_valid, target_valid = valid_batch
                input_valid = input_valid.to(device=self.device)
                target_valid = target_valid.to(device=self.device)

                # input_valid = self.transforms(input_valid)

                output_valid, output_valid_aux1, output_valid_aux2 = self.model(input_valid)
                loss = self.loss_fn(output_valid, target_valid)
                loss_aux1 = self.loss_fn(output_valid_aux1, target_valid)
                loss_aux2 = self.loss_fn(output_valid_aux2, target_valid)
                loss += 0.3 * (loss_aux1 + loss_aux2)
                loss_valid += loss.item()


                predicted_valid = torch.argmax(output_valid, dim=1)
                num_corrects_valid += torch.sum(torch.eq(predicted_valid, target_valid)).item()

                num_valid_samples += len(input_valid)
                num_valids += 1

        valid_loss = loss_valid / num_valids
        valid_accuracy = num_corrects_valid / num_valid_samples * 100

        return valid_loss, valid_accuracy
    
    def train_loop(self):
        """
        Whole training loop.
        Each epoch, call train function and validation function to train, test model.
        Not only print result at local terminal to check how training is settled, but also save log on wandb
        Check validation loss, then decide to continue and save or break loop.
        """
        epochs = self.wandb.config.epochs
        runtime = datetime.now()
        early_stopping = EarlyStopping(
            patience=self.wandb.config.early_stop_patience,
            delta=self.wandb.config.early_stop_delta,
            project_name=self.project_name,
            checkpoint_file_path=self.checkpoint_file_path,
            runtime=runtime
        )

        for epoch in range(1, epochs):
            train_loss, train_accuracy = self.do_train()

            if epoch == 1 or epoch % self.wandb.config.validation_intervals == 0:
                valid_loss, valid_accuracy = self.do_valid()

                message, early_stop = early_stopping.check_and_save(valid_loss, self.model)

                print(
                    f"[Epoch {epoch:>4}] "
                    f"T_loss: {train_loss:7.5f}, "
                    f"T_acc: {train_accuracy:6.4f} | "
                    f"V_loss: {valid_loss:7.5f}, "
                    f"V_acc: {valid_accuracy:6.4f} | "
                    f"{message}"
                )

                self.wandb.log({
                    "Epoch": epoch,
                    "Training Loss": train_loss,
                    "Training Accuracy(%)": train_accuracy,
                    "Validation Loss": valid_loss,
                    "Validation Accuracy(%)": valid_accuracy
                })

                if early_stop:
                    break
