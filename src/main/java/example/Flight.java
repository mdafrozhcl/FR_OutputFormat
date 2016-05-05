package example;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Flight implements WritableComparable<Flight> {

	String Year, Month, DayofMonth, DepTime, ArrTime, UniqueCarrier, FlightNum, ActualElapsedTime, ArrDelay, DepDelay,
			Origin, Dest;

	public Flight() {
		this.Year = "";
		this.Month = "";
		this.DayofMonth = "";
		this.DepTime = "";
		this.ArrTime = "";
		this.UniqueCarrier = "";
		this.FlightNum = "";
		this.ActualElapsedTime = "";
		this.ArrDelay = "";
		this.DepDelay = "";
		this.Origin = "";
		this.Dest = "";
	}

	public String getYear() {
		return Year;
	}

	public void setYear(String year) {
		Year = year;
	}

	public String getMonth() {
		return Month;
	}

	public void setMonth(String month) {
		Month = month;
	}

	public String getDayofMonth() {
		return DayofMonth;
	}

	public void setDayofMonth(String dayofMonth) {
		DayofMonth = dayofMonth;
	}

	public String getDepTime() {
		return DepTime;
	}

	public void setDepTime(String depTime) {
		DepTime = depTime;
	}

	public String getArrTime() {
		return ArrTime;
	}

	public void setArrTime(String arrTime) {
		ArrTime = arrTime;
	}

	public String getUniqueCarrier() {
		return UniqueCarrier;
	}

	public void setUniqueCarrier(String uniqueCarrier) {
		UniqueCarrier = uniqueCarrier;
	}

	public String getFlightNum() {
		return FlightNum;
	}

	public void setFlightNum(String flightNum) {
		FlightNum = flightNum;
	}

	public String getActualElapsedTime() {
		return ActualElapsedTime;
	}

	public void setActualElapsedTime(String actualElapsedTime) {
		ActualElapsedTime = actualElapsedTime;
	}

	public String getArrDelay() {
		return ArrDelay;
	}

	public void setArrDelay(String arrDelay) {
		ArrDelay = arrDelay;
	}

	public String getDepDelay() {
		return DepDelay;
	}

	public void setDepDelay(String depDelay) {
		DepDelay = depDelay;
	}

	public String getOrigin() {
		return Origin;
	}

	public void setOrigin(String origin) {
		Origin = origin;
	}

	public String getDest() {
		return Dest;
	}

	public void setDest(String dest) {
		Dest = dest;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(Year);
		out.writeUTF(Month);
		out.writeUTF(DayofMonth);
		out.writeUTF(DepTime);
		out.writeUTF(ArrTime);
		out.writeUTF(UniqueCarrier);
		out.writeUTF(FlightNum);
		out.writeUTF(ActualElapsedTime);
		out.writeUTF(ArrDelay);
		out.writeUTF(DepDelay);
		out.writeUTF(Origin);
		out.writeUTF(Dest);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.Year = in.readUTF();
		this.Month = in.readUTF();
		this.DayofMonth = in.readUTF();
		this.DepTime = in.readUTF();
		this.ArrTime = in.readUTF();
		this.UniqueCarrier = in.readUTF();
		this.FlightNum = in.readUTF();
		this.ActualElapsedTime = in.readUTF();
		this.ArrDelay = in.readUTF();
		this.DepDelay = in.readUTF();
		this.Origin = in.readUTF();
		this.Dest = in.readUTF();
	}

	@Override
	public int compareTo(Flight o) {
		int result = (this.getYear() + this.getMonth() + this.getDayofMonth())
				.compareTo(o.getYear() + o.getMonth() + o.getDayofMonth());
		if (result == 0) {
			result = (-1) * (Integer.parseInt(this.getArrDelay()) - Integer.parseInt(o.getArrDelay()));
		}
		return result;
	}

	@Override
	public String toString() {

		return this.getYear() + "," + this.getMonth() + "," + this.getDayofMonth() + "," + this.getDepTime() + ","
				+ this.getArrTime() + "," + this.getUniqueCarrier() + "," + this.getFlightNum() + ","
				+ this.getActualElapsedTime() + "," + this.getArrDelay() + "," + this.getDepDelay() + ","
				+ this.getOrigin() + "," + this.getDest();
	}

}
