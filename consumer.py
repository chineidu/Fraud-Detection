import logging
from typing import Any

from quixstreams import Application
from quixstreams.dataframe.dataframe import StreamingDataFrame
from quixstreams.models.topics.topic import Topic


def main() -> None:
    """Run a consumer that processes weather data and sends processed data to another Kafka topic."""
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)

    app = Application(
        broker_address="localhost:9092",
        loglevel="DEBUG",
        consumer_group="weather_reader",
        auto_offset_reset="earliest",
    )

    input_topic: Topic = app.topic("weather_data_demo")
    output_topic: Topic = app.topic("processed_weather_data")

    def process_weather_data(data: dict[str, Any]) -> dict[str, Any]:
        """Process weather data to extract and convert temperature information."""
        temp_celcius: float | None = data.get("current", {}).get("temperature_2m")
        temp_fahrenheit: float | None = None
        temp_kelvin: float | None = None

        if temp_celcius is not None:
            temp_fahrenheit = round((temp_celcius * 9 / 5) + 32, 2)
            temp_kelvin = round(temp_celcius + 273.15, 2)

        return {
            "temperature_c": temp_celcius,
            "temperature_f": temp_fahrenheit,
            "temperature_k": temp_kelvin,
        }

    logger.info("Processing weather data")
    streaming_df: StreamingDataFrame = app.dataframe(topic=input_topic).apply(process_weather_data)
    output_df: StreamingDataFrame = streaming_df.to_topic(output_topic)

    app.run(output_df)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
