/**
 * 
 */

package de.uni_potsdam.hpi.fgnaumann.lsdd;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.io.RecordInputFormat;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.common.type.base.parser.DecimalTextIntParser;
import eu.stratosphere.pact.common.type.base.parser.VarLengthStringParser;

/**
 * A Stratosphere-Pact-Implementation of "A fast approach for parallel
 * deduplication on multicore processors" - Guilherme Dal Bianco et al.
 * 
 * @author fabian.tschirschnitz@student.hpi.uni-potsdam.de
 * 
 */
public class MultiBlocking implements PlanAssembler, PlanAssemblerDescription {

	@Override
	public Plan getPlan(final String... args) {
		// parse program parameters
		//4 file:///home/fabian/lsdd/data/freedb_discs.csv file:///home/fabian/lsdd/data/freedb_tracks.csv file:///home/fabian/lsdd/out
		final int noSubtasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		final String inputFileDiscs = (args.length > 1 ? args[1] : "");
		final String inputFileTracks = (args.length > 2 ? args[2] : "");
		final String output = (args.length > 3 ? args[3] : "");

		// create DataSourceContract for Orders input
		//disc_id;freedbdiscid;"artist_name";"disc_title";"genre_title";"disc_released";disc_tracks;disc_seconds;"disc_language"
		//7;727827;"Tenacious D";"Tenacious D";"Humour";"2001";19;2843;"eng"
		FileDataSource discs = new FileDataSource(RecordInputFormat.class, inputFileDiscs, "Discs");
		RecordInputFormat.configureRecordFormat(discs).recordDelimiter('\n').fieldDelimiter(';')
				.field(DecimalTextIntParser.class, 0) // disc_id
				.field(DecimalTextIntParser.class, 1) // freedbdiscid
				.field(VarLengthStringParser.class, 2) // "artist_name"
				.field(VarLengthStringParser.class, 3) // "disc_title"
				.field(VarLengthStringParser.class, 4) // "genre_title"
				.field(VarLengthStringParser.class, 5) // "disc_released"
				.field(DecimalTextIntParser.class, 6) // disc_tracks
				.field(DecimalTextIntParser.class, 7) // disc_seconds
				.field(VarLengthStringParser.class, 8); // "disc_language"

		//
		MapContract firstBlockingStepMapper = MapContract.builder(FirstBlockingStep.class)
				.input(discs)
				.name("apply blocking functions to records")
				.build();
			FileDataSink out = new FileDataSink(RecordOutputFormat.class, output, firstBlockingStepMapper, "Output");
			RecordOutputFormat.configureRecordFormat(out)
				.recordDelimiter('\n')
				.fieldDelimiter(' ')
				.lenient(true)
				.field(PactString.class, 0)
				.field(PactString.class, 1);
		
		// assemble the PACT plan
		Plan plan = new Plan(out, "MultiBlocking");
		plan.setDefaultParallelism(noSubtasks);
		return plan;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getDescription() {
		return "Parameters: [noSubStasks], [discs], [tracks], [output]";
	}
	
	/**
	 * Mapper that applies the blocking functions to each record
	 * @author fabian.tschirschnitz@student.hpi.uni-potsdam.de
	 *
	 */
	public static class FirstBlockingStep extends MapStub
	{
		// initialize reusable mutable objects
		private final PactRecord outputRecord = new PactRecord();
		
		@Override
		public void map(PactRecord record, Collector<PactRecord> collector)
		{
			PactString genre = record.getField(4, PactString.class);
			PactString year = record.getField(5, PactString.class);
			PactString blockingKey = new PactString(genre.getValue()+year.getValue());
			outputRecord.setField(0, blockingKey);
			outputRecord.setField(1, new PactString("foo"));
			collector.collect(outputRecord);
		}
	}
}


