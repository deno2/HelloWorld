package storm.cookbook;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class HelloWorldTopology {

	/**
	 * @param args
	 * mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=storm.cookbook.HelloWorldTopology
	 */
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("randomHelloWorld", new HelloWorldSpout(), 100);
		builder.setBolt("HelloWorldBolt", new HelloWorldBolt(), 3).shuffleGrouping("randomHelloWorld");
		
		Config conf = new Config();
		conf.setDebug(true);
		
		if(args != null && args.length > 0){
			conf.setNumWorkers(3);
			
			try {
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			}
			
		}else{
			System.out.println("Note: Launching Local Cluster Man!");
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, builder.createTopology());
			
			Utils.sleep(30000);
			cluster.killTopology("test");
			cluster.shutdown();
		}
		
		
	}

}
