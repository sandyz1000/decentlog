#! /usr/bin/env python
import argparse
from decentlog import DweetHttpListener, KafkaListener


def _main():
    parser = argparse.ArgumentParser(description="Decentralized logging using dweet server")
    parser.add_argument(
        "--mode",
        type=str,
        default='LISTENER',
        help="To start program in PUBLISHER or LISTENER mode",
    )
    parser.add_argument(
        "--no_streaming",
        action="store_true",
        help="Use polling instead of streaming API provided by dweet.io",
    )
    parser.add_argument(
        "--channel",
        type=str,
        help="Channel name where you want log to listen",
        required=True,
    )
    parser.add_argument(
        "--settings_file",
        type=str,
        help="Settings file that will be used for initial loggind setup",
        default="dev_settings.py",
    )

    parser.add_argument(
        "--listener-type",
        type=str,
        help="",
        choices=["kafka", "dweet"],
        default="kafka"
    )
    args = parser.parse_args()
    assert args.channel, "Channel name should not be empty"
    try:
        listener = KafkaListener()
        for res in listener.listen_channel_output():
            print(res)
    except KeyboardInterrupt:
        print("Closing .....")


if __name__ == "__main__":
    _main()
