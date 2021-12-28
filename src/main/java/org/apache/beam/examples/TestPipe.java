package org.apache.beam.examples;

import org.apache.beam.examples.common.ExampleUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.Regex;

/**
 * //run with 
 * mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.TestPipe \
 *    -Dexec.args="--inputFile=pom.xml --output=counts" -Pdirect-runner	 
 */

public class TestPipe {

	public interface TestPipeOptions extends PipelineOptions {

    @Description("Path of the file to read from")
    @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
    String getInputFile();

    void setInputFile(String value);

    /** Set this required option to specify where to write the output. */
    @Description("Path of the file to write to")
    @Default.String("/mnt/c/Users/ftw712/desktop/")
    String getOutput();

    void setOutput(String value);
  }
	
  static void runTestPipe(TestPipeOptions options) {
    Pipeline p = Pipeline.create(options);

    p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
	 .apply(Regex.replaceAll(" ","dog"))
	 .apply("WriteCounts", TextIO.write().to(options.getOutput()));
    
    p.run().waitUntilFinish();
  }		

	public static void main(String[] args) {
		for (int x = 0; x < args.length; x++) System.out.println(args[x]);
		TestPipeOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(TestPipeOptions.class);
		runTestPipe(options);
	}

} 