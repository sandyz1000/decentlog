#! /usr/bin/env python
import argparse
from decentlog import listen_channel_output


def _main():
    parser = argparse.ArgumentParser(description="Decentralized logging using dweet server")
    parser.add_argument(
        "--mode",
        type=str,
        default='LISTENER',
        help="To start program in PUBLISHER or LISTENER mode"
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
        required=True
    )

    args = parser.parse_args()
    assert args.channel, "Channel name should not be empty"
    try:
        for res in listen_channel_output(args.channel, longpoll=args.no_streaming):
            print(res)
    except KeyboardInterrupt:
        print("Closing .....")


if __name__ == "__main__":
    _main()
