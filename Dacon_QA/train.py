import torch
import wandb
from trainer import Trainer
from src.model import GoogleNet
from torch import optim
from datetime import datetime
from src.dataset import get_CIFAR10, get_CIFAR100
import os
from pathlib import Path

def train(args):
    config = {
        'epochs': args.epochs,
        'batch_size': args.batch_size,
        'learning_rate': args.learning_rate,
        'weight_decay': args.weight_decay,
        'validation_intervals': args.validation_intervals,
        'early_stop_patience': args.early_stop_patience,
        'early_stop_delta': args.early_stop_delta
    }

    runtime = datetime.now().astimezone().strftime("%m-%d_%H-%M-%S")
    project = "GoogleNet"
    wandb.init(
        mode="online" if args.wandb == True else "disabled",
        project=project,
        name=runtime,
        config=config
    )

    print(wandb.config)

    device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
    print(f"Training on device {device}")

    train_data_loader, valid_data_loader, CIFAR10_transforms = get_CIFAR10(train=True)
    # train_data_loader, valid_data_loader, CIFAR10_transforms = get_CIFAR100(train=True)

    model = GoogleNet(n_outputs=10)
    # model = GoogleNet(n_outputs=100)
    model = model.to(device)
    # wandb.watch(model)

    optimizer = optim.Adam(model.parameters(), lr=wandb.config.learning_rate, weight_decay=wandb.config.weight_decay)

    checkpoint_file_path = os.path.join(Path(__file__).parent, "model")
    if not os.path.isdir(checkpoint_file_path):
        os.mkdir(checkpoint_file_path)

    from torchinfo import summary
    summary(
        model=model, input_size=(1, 3, 32, 32),
        col_names=["kernel_size", "input_size", "output_size", "num_params", "mult_adds"]
    )

    trainer = Trainer(project, model, optimizer, train_data_loader, valid_data_loader, CIFAR10_transforms, wandb, device, checkpoint_file_path)

    trainer.train_loop()

    wandb.finish()
    
    print("### Training Done! ###")