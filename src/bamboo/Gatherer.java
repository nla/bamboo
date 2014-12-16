package bamboo;

public class Gatherer {

	final PandasDB db = PandasDB.open();
	int pollingInterval = 1000;
	
	public void run() {
		while (true) {
			try {
				Thread.sleep(pollingInterval);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}

			
		}
	}

	public static void main(String args[]) {
		new Gatherer().run();
	}

}
