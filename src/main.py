
import argparse
import os
from configParser import (
    ler_config_broker,
    ler_config_publisher,
    ler_config_subscriber,
)
from broker import Broker
from publisher import Publisher
from subscriber import Subscriber


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sistema de publicação/subscrição"
    )

    parser.add_argument(
        "papel",
        choices=["broker", "publisher", "subscriber"],
        help="Papel do processo",
    )

    parser.add_argument(
        "--id",
        type=int,
        required=True,
        help="Identificador do processo",
    )

    parser.add_argument(
        "--config",
        type=str,
        required=True,
        help="Caminho absoluto do ficheiro de configuração",
    )

    args = parser.parse_args()

    if not os.path.isabs(args.config):
        parser.error("--config tem de ser um caminho absoluto")

    if args.papel == "broker":
        config = ler_config_broker(args.config, args.id)
        processo = Broker(args.id, config)

    elif args.papel == "publisher":
        config = ler_config_publisher(args.config, args.id)
        processo = Publisher(config)

    else:
        config = ler_config_subscriber(args.config, args.id)
        processo = Subscriber(config)

    processo.executar()


if __name__ == "__main__":
    main()
