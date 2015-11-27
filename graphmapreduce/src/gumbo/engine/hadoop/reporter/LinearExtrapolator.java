package gumbo.engine.hadoop.reporter;

public class LinearExtrapolator {
	
	private double a;
	private double b;

	public LinearExtrapolator() {
		a = 1;
		b = 0;
	}
	
	public void load(double a, double b){
		this.a = a;
		this.b = b;
	}
	
	public void loadValues(double x1, double y1, double x2, double y2){
		
		double a = (y2-y1)/(double)(x2-x1);
		double b = y1 - a*x1;
		load(a,b);
		
	}
	
	public double extrapolate(double x) {
		return a * x + b;
	}

}
