package org.apache.beam.samples;

import java.util.Arrays;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.pubsub.v1.ProjectSubscriptionName;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptor;

public class StreamingBeamSQL {

  public interface Options extends StreamingOptions {
    @Description("Pub/Sub subscription to read from.")
    @Validation.Required
    String getInputSubscription();
    void setInputSubscription(String value);

    @Description("BigQuery table to write to, in the form 'project:dataset.table' or 'dataset.table'.")
    @Default.String("beam_samples.streaming_beam_sql")
    String getOutputTable();
    void setOutputTable(String value);
  }

  // ddmmyy, hhmmss.ssをTimestamp形式(yyyy-mm-ddThh:mm:ss.ssZ)の文字列に変換
  private static String FormatTimestamp(String date, String time) {
    return String.format("20%s-%s-%sT%s:%s:%s.%sZ",
                            date.substring(4, 6),  // yy
                            date.substring(2, 4),  // mm
                            date.substring(0, 2),  // dd
                            time.substring(0, 2),  // hh
                            time.substring(2, 4),  // mm
                            time.substring(4, 6),  // ss
                            time.substring(7));    // 小数点以下
  }

  // 60進法の緯度経度をWell-Known Text形式(POINT(10進法経度 緯度))の文字列に変換
  private static String FormatWKT(String latitude, String longitude) {
    return String.format("POINT(%s %s)", Format60to10(longitude), Format60to10(latitude));
  }

  // 60進法の緯度経度(dddmm.mmmm, ddd:度, mm.mmmm:分)を10進法に変換
  private static String Format60to10(String target) {
    double d = Double.parseDouble(target);
    int x = (int)(d / 100);                 // 度を取得
    d = ( d / 100 - (double)x ) * 100 / 60; // 分を度に変換
    d = (double)x + d;                      // 分と度を足す
    return String.valueOf(d);
  }

  public static void main(final String[] args) {

    final Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    options.setStreaming(true);
    final var project = options.as(GcpOptions.class).getProject();
    final var subscription = ProjectSubscriptionName.of(project, options.getInputSubscription()).toString();
    final var pipeline = Pipeline.create(options);

    pipeline

        // Cloud Pub/Subからのメッセージ読込を適用
        .apply("Read messages from Pub/Sub", PubsubIO.readStrings().fromSubscription(subscription))

        // CSV形式からBigQuery_TableRows形式への変換を適用
        .apply("Convert to BigQuery TableRow", MapElements.into(TypeDescriptor.of(TableRow.class)).via(row -> {
          return new TableRow()
            .set("device_id", row.split(",", 5)[0])
            .set("timestamp", FormatTimestamp(row.split(",", 5)[1], row.split(",", 5)[2]))
            .set("location",  FormatWKT(row.split(",", 5)[3], row.split(",", 5)[4]));
        }))

        // 起動時パラメータで指定されたBigQueryテーブルへの書込を適用
        // テーブルが存在しない場合はTableSchema情報を元に作成
        .apply("Write to BigQuery", BigQueryIO.writeTableRows()
            .to(options.getOutputTable())
            .withSchema(new TableSchema().setFields(Arrays.asList(
                new TableFieldSchema().setName("device_id").setType("STRING"),
                new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"),
                new TableFieldSchema().setName("location").setType("GEOGRAPHY"))))
            .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND));

    // パイプライン実行
    pipeline.run();
  }
}