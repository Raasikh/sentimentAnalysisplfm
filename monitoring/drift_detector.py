import structlog 
import polars as pl
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset
from prometheus_client import Gauge

logger= structlog.get_logger("drift_detector")



DRIFT_SCORE = Gauge(
    "data_drift_score",
    "Overall data drift score (0 = no drift, 1 = full drift)",
)


FEATURE_DRIFT= Gauge(
    "feature_drift_detected",
    "Whether drift was detected for a specific feature (0 = no drift, 1 = drift)",
    ["feature_name"],
)



class DriftDetector:
    def __init__(self, reference_df:pl.DataFrame):
        self.reference_df= reference_df
        self.drift_threshold= 0.5  # This can be tuned based on your needs


    def check_drift(self,current_df: pl.DataFrame):
        """
        Check for data drift between the reference dataset and the current dataset.
        """
        monitor_columns= ["score", "num_comments", "sentiment_score"]

        shared_cols= [
            c for c in monitor_columns if c in self.reference_df.columns 
            and c in current_df.columns

        ]

        if not shared_cols:
            logger.warning("No shared columns for drift detection", monitor_columns=monitor_columns)
            return {"drift": False, "score": 0.0, "details": {}}
        

        ref_pd= self.reference_df.select(shared_cols).to_pandas()
        curr_pd= current_df.select(shared_cols).to_pandas()
        report= Report(metrics=[DataDriftPreset()])

        report.run(reference_data= ref_pd, current_data= curr_pd)

        result= report.as_dict()

        drift_summary= result["metrics"][0]["result"]
        
        overall_Score= drift_summary["share_of_drifted_columns"]

        drifted= overall_Score > self.drift_threshold


        DRIFT_SCORE.set(overall_Score)

        details={}


        for col_name, col_data in drift_summary["metrics"].items():
            if col_name in shared_cols:
                is_drifted= col_data["drift_detected"]
                details[col_name]= {
                    "drifted": is_drifted,
                    "drift_score": col_data.get("drift_score", None),
                }
                FEATURE_DRIFT.labels(feature_name=col_name).set(1.0 if is_drifted else 0.0)

        logger.info("diift detection result", drifted=drifted, overall_score=overall_Score, details=details)

        return {"drift": drifted, "score": overall_Score, "details": details}
    


_detector= None


def get_detector(reference_df: pl.DataFrame) -> DriftDetector:
    global _detector
    if _detector is None:
        _detector= DriftDetector(reference_df)
        logger.info("Initialized DriftDetector with reference dataset", num_rows=reference_df.height)
    return _detector




def run_drift_check(
    current_df: pl.DataFrame,
    reference_df: pl.DataFrame | None = None,
) -> dict | None:
    """Run drift check, using first batch as reference if needed."""
    global _detector

    if _detector is None:
        if reference_df is not None:
            _detector = DriftDetector(reference_df)
            logger.info("Initialized DriftDetector with reference dataset", num_rows=reference_df.height)
        else:
            # Use current batch as the reference (first run)
            _detector = DriftDetector(current_df)
            logger.info("First batch stored as drift reference")
            return None

    return _detector.check_drift(current_df)