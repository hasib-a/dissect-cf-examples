package uk.ac.ljmu.fet.cs.cloud.examples.autoscaler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import uk.ac.ljmu.fet.cs.cloud.examples.autoscaler.*;

public class CustomVMAutoScaler extends VirtualInfrastructure {

	private String applicationName = "";
	/*
	 * Minimum CPU utilisation as a percentage of a single virtual machine
	 * that we still want to keep in the virtual infrastructure. In this case
	 * I have set the value to 5%.
	 */
	public static final double minUtil = .05;
	/*
	 * Maximum CPU utilisation as a percentage of ALL virtual machines of the 
	 * same kind of executable. Under this value no new VM will be needed. 
	 * In this case I have set the value to 70% to allow for some overhead.
	 */
	public static final double maxUtil = .7;
	/*
	 * This data structure will keep track of how many times the last VM was not used.
	 */
	private final HashMap<VirtualMachine, Integer> unusedVMMap = new HashMap<VirtualMachine, Integer>();


	public CustomVMAutoScaler(IaaSService cloud) {
		super(cloud);
		// TODO Auto-generated constructor stub
		/*
		 * My auto scaling algorithm for the course work.
		 * ----------------------------------------------
		 * The logic for this algorithm:
		 * Parameters for minimum CPU utilisation and maximum utilisation are set at 5% and 70% respectively.
		 * 
		 * If CPU utilisation of a singular virtual machine falls below minimum (5%), destroy the VM unless it is the last one. 
		 * This is to allow keep one of each virtual machine kind in case new jobs come in. If this last virtual machine does
		 * not receive a job within 5 simulated minutes, then remove this virtual machine. This is to reduce costs on a pay per
		 * minute billing. 
		 * 
		 * If average CPU utilisation of all virtual machines is more than the maximum utilisation threshold (70%) then scale up
		 * and create a new virtual machine of the same kind of virtual machine.
		 * 
		 */
	}

	@Override
	public void tick(long fires) {		

		if(applicationName.equals("")) {


			final Iterator<String> kinds = vmSetPerKind.keySet().iterator();
			while(kinds.hasNext()) {
				final String kind = kinds.next();
				final ArrayList<VirtualMachine> vmset = vmSetPerKind.get(kind);


				if(vmset.isEmpty()) {

					requestVM(kind);

					//Remember application
					applicationName = kind;
					//System.out.println(applicationName);
					continue;
				} else if (vmset.size() == 1) {
					final VirtualMachine onlyMachine = vmset.get(0);
					// check not to destroy last virtual machine of a kind
					if (onlyMachine.underProcessing.isEmpty() && onlyMachine.toBeAdded.isEmpty() ) {
						Integer k = unusedVMMap.get(onlyMachine);
						if (k == null) {
							unusedVMMap.put(onlyMachine, 1);
						} else {
							k++;
							//check up to 5 tics (10 min)
							if (k < 5) {
								unusedVMMap.put(onlyMachine, k);
							} else {
								// if VM is unused for 10 minutes, we destory the VM
								unusedVMMap.remove(onlyMachine);
								destroyVM(onlyMachine);
								kinds.remove();
							}
						}
						continue;
					}
					unusedVMMap.remove(onlyMachine);
					// now to check if we need to scale up the number of VM's
				} else {
					boolean destroyed = false;
					for (int i = 0; i < vmset.size(); i++) {
						final VirtualMachine vm = vmset.get(i);
						if (vm.underProcessing.isEmpty() && vm.toBeAdded.isEmpty()) {
							// VM not currently processing anything and therefore not being used
							if(getHourlyUtilisationPercForVM(vm) < minUtil) {
								//VM load is under 5% in the last hour, we remove this now.
								destroyVM(vm);
								destroyed = true;
								i--;
							}
						}
					}
					if (destroyed == true) {
						//no need to do anything if VM is destroyed.
						continue;
					}
				}
				double intermittentUtilCalc = 0;
				for (VirtualMachine vm : vmset) {
					intermittentUtilCalc += getHourlyUtilisationPercForVM(vm);
				}
				if (intermittentUtilCalc / vmset.size() > maxUtil) {
					requestVM(kind);
				}
			}
		}
	}
}