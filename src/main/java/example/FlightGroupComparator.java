package example;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FlightGroupComparator extends WritableComparator {
	public FlightGroupComparator() {
		super(Flight.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		Flight l = (Flight) a;
		Flight r = (Flight) b;
		return (l.getYear() + l.getMonth() + l.getDayofMonth())
				.compareTo(r.getYear() + r.getMonth() + r.getDayofMonth());
	}

}
