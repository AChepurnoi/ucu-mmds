import os

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--title", type=str, required=True)
    args = parser.parse_args()

    print(args.title)