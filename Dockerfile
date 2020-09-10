# Cloud BuildでDockerイメージをビルドしContainer Registoryに保存
FROM gcr.io/dataflow-templates-base/java11-template-launcher-base:latest

ENV FLEX_TEMPLATE_JAVA_MAIN_CLASS="org.apache.beam.samples.StreamingBeamSQL"
ENV FLEX_TEMPLATE_JAVA_CLASSPATH="/template/pipeline.jar"

COPY ./target/streaming-beam-sql-1.0.jar ${FLEX_TEMPLATE_JAVA_CLASSPATH}
