package de.uni_potsdam.hpi.fgnaumann.lsdd.stubs;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;

/**
 * Matcher that compares the result with the gold standard and only emits
 * tp's
 * 
 * @author fabian.tschirschnitz@student.hpi.uni-potsdam.de
 * 
 */
public class ValidatorStep extends MatchStub {

	@Override
	public void match(PactRecord value1, PactRecord value2,
			Collector<PactRecord> out) throws Exception {
		out.collect(value1.createCopy());
	}

}