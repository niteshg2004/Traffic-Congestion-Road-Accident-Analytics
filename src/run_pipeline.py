"""Single entry point that runs the whole pipeline in order.

  python -m src.run_pipeline                 # all stages on data/raw/*.csv
  python -m src.run_pipeline --stage model   # one stage only
  python -m src.run_pipeline --sample        # synthetic smoke test (not real data)
Stages: preprocess, analysis, hotspot, risk, model, all
"""
import argparse
import os
import sys
import time

STAGES = ["preprocess", "analysis", "hotspot", "risk", "model"]


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Traffic accident Big Data pipeline")
    p.add_argument("--stage", default="all", choices=STAGES + ["all"])
    p.add_argument("--sample", action="store_true",
                   help="use the SYNTHETIC sample file (outputs go to *_sample folders)")
    args = p.parse_args(argv)
    if args.sample:
        os.environ["CA3_SAMPLE"] = "1"      # must be set before config is imported

    from src import config
    from src.utils import (DatasetNotFoundError, EmptyDatasetError, SchemaError,
                           get_logger)
    log = get_logger("pipeline")
    todo = STAGES if args.stage == "all" else [args.stage]
    t0 = time.time()
    try:
        config.ensure_dirs()
        if args.sample and not config.SAMPLE_FILE.exists():
            from src.make_sample import make
            make().to_csv(config.SAMPLE_FILE, index=False)
        from src.spark_session import get_spark
        spark = get_spark()
        log.info("Spark %s started | UI: %s | mode: %s", spark.version,
                 spark.sparkContext.uiWebUrl, "SAMPLE" if args.sample else "FULL DATASET")

        grid = None
        if "preprocess" in todo:
            from src.preprocessing import run_preprocessing
            run_preprocessing(spark)
        if "analysis" in todo:
            from src.analysis import run_analysis
            run_analysis(spark)
        if "hotspot" in todo or "risk" in todo:
            from src.hotspot_analysis import run_hotspot_analysis
            grid = run_hotspot_analysis(spark)
        if "risk" in todo:
            from src.risk_score import run_risk_score
            run_risk_score(spark, grid)
        if "model" in todo:
            from src.model import run_model
            run_model(spark)
        log.info("DONE in %.1f min. Results in: %s", (time.time() - t0) / 60, config.RESULTS_DIR)
        log.info("Start the dashboard with: streamlit run dashboard/app.py")
        if os.environ.get("CA3_PAUSE") == "1":   # keep Spark UI alive for screenshots
            input("Spark UI is still running (see URL above). Press Enter to exit...")
        spark.stop()
        return 0
    except (DatasetNotFoundError, SchemaError, EmptyDatasetError) as exc:
        print(f"\nERROR: {exc}\n", file=sys.stderr)
        return 1
    except Exception as exc:
        print("\nUNEXPECTED ERROR - see traceback below. Common causes: Java not "
              "installed (need Java 11/17), too little memory (set SPARK_DRIVER_MEMORY), "
              "or a corrupted dataset.\n", file=sys.stderr)
        raise


if __name__ == "__main__":
    sys.exit(main())
