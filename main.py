import sys

from weekly_stats.pipeline import main


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
        sys.exit(1)
    except Exception as exc:
        print(f"\nFATAL: {exc}")
        sys.exit(1)
