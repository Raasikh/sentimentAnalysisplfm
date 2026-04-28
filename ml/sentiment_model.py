import structlog
from transformers import pipeline


logger= structlog.get_logger("sentiment_model")

MODEL_NAME= "distilbert-base-uncased-finetuned-sst-2-english"

class SentimentModel:
    def __init__(self, model_name:str= MODEL_NAME):
        self.model_name= model_name
        self.pipeline= None

    def load_model(self):
        """
        Load the sentiment analysis model pipeline.
        """
        self.pipeline= pipeline("sentiment-analysis", model=self.model_name, device=-1)
        logger.info("Sentiment model loaded", model_name=self.model_name)

    def predict_sentiment(self, text:str)-> str:
        """
        Predict the sentiment of the given text using the loaded model pipeline.
        """
        if self.pipeline is None:
            raise ValueError("Model pipeline not loaded. Call load_model() first.")
        
        result = self.pipeline(text, truncation=True, max_length=512)[0]

        return {
            "label": result["label"],
            "score": round(result["score"], 4),
        }
    

    def predict_batch(self, texts: list[str]) -> list[dict]:
        """
        Predict the sentiment for a batch of texts.
        """
        if self.pipeline is None:
            self.load_model()

        valid= [(i, text) for i, text in enumerate(texts) if text and text.strip()]

        if not valid:
            return [{"label": "NEUTRAL", "score": 0.0} for _ in texts]
        
        indices, valid_texts= zip(*valid)

        results= self.pipeline(list(valid_texts), truncation=True, max_length=512, batch_size=32)

        output= [{"label": "NEUTRAL", "score": 0.0} for _ in texts]
        for idx, res in zip(indices, results):
            output[idx]= {
                "label": res["label"],
                "score": round(res["score"], 4),
            }


        return output