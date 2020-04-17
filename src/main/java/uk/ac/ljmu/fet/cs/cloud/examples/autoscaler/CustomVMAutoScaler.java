package uk.ac.ljmu.fet.cs.cloud.examples.autoscaler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import uk.ac.ljmu.fet.cs.cloud.examples.autoscaler.*;

public class CustomVMAutoScaler extends VirtualInfrastructure {
	/*
	 * Minimum CPU utilisation as a percentage of a single virtual machine
	 * that we still want to keep in the virtual infrastructure. In this case
	 * I have set the value to 20%.
	 */
	public static final double minUtil = .2;
	/*
	 * Maximum CPU utilisation as a percentage of ALL virtual machines of the 
	 * same kind of executable. Under this value no new VM will be needed. 
	 * In this case I have set the value to 70% to allow for some overhead.
	 */
	public static final double maxUtil = .70;
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
		 * Parameters for minimum CPU utilisation and maximum utilisation are set at 20% and 70% respectively.
		 * 
		 * If CPU utilisation of a singular virtual machine falls below minimum (20%), destroy the VM unless it is the last one. 
		 * This is to allow keep one of each virtual machine kind in case new jobs come in. If this last virtual machine does
		 * not receive a job within 58 minutes simulated minutes, then remove this virtual machine. The value of 58 minutes was 
		 * used to check the virtual machine before the hour is up, not to incur more cost for another hour on a pay-per-hour billing.
		 * 
		 * If average CPU utilisation of all virtual machines is more than the maximum utilisation threshold (70%) then scale up
		 * and create a new virtual machine of the same kind of virtual machine.
		 * 
		 */
	}

	@Override
	public void tick(long fires) {		
			final Iterator<String> kinds = vmSetPerKind.keySet().iterator();
			while(kinds.hasNext()) {
				final String kind = kinds.next();
				final ArrayList<VirtualMachine> vmset = vmSetPerKind.get(kind);


				if(vmset.isEmpty()) {

					requestVM(kind);
					
					continue;
				} else if (vmset.size() == 1) {
					final VirtualMachine soleMachine = vmset.get(0);
					// check not to destroy last virtual machine of a kind
					if (soleMachine.underProcessing.isEmpty() && soleMachine.toBeAdded.isEmpty() ) {
						Integer k = unusedVMMap.get(soleMachine);
						if (k == null) {
							unusedVMMap.put(soleMachine, 1);
						} else {
							k++;
							//check up to 29 ticks (58 min)
							if (k < 29) {
								unusedVMMap.put(soleMachine, k);
							} else {
								//if ()
								// if VM is unused for 58 minutes, we destory the VM
								unusedVMMap.remove(soleMachine);
								destroyVM(soleMachine);
							}
						}
						// continue to next block of code, no more VM's needed as there is 0 processing going on
						continue;
					}
					unusedVMMap.remove(soleMachine);
					// now to check if we need to scale up the number of VM's
				} else {
					boolean isDestroyed = false;
					for (int j = 0; j < vmset.size(); j++) {
						final VirtualMachine vm = vmset.get(j);
						if (vm.underProcessing.isEmpty() && vm.toBeAdded.isEmpty()) {
							// VM not currently processing anything and therefore not being used
							if(getHourlyUtilisationPercForVM(vm) < minUtil) {
								//VM load is under 20% in the last hour, we remove this now.
								destroyVM(vm);
								isDestroyed = true;
								j--;
							}
						}
					}
					if (isDestroyed == true) {
						//no need to do anything if VM is destroyed.
						continue;
					}
				}
				// scale up logic, check if we need more vm's than we current have in the pool
				double intermittentUtilCalc = 0;
				for (VirtualMachine vm : vmset) {
					intermittentUtilCalc += getHourlyUtilisationPercForVM(vm);
				}
				if (intermittentUtilCalc / vmset.size() > maxUtil) {
					requestVM(kind);
				}
			}
			startAutoScaling();
	}
}