# Easy Yarn 

## What's Easy Yarn?
Easy yarn is a framework that abstracts the yarn framework complexity and provide a simpler API allowing developers to focus more on their application logic.

## Why not direct YARN  ?
Apache Yarn is very powerful and flexible, but this comes at a cost. YARN can be quite difficult to use and requires a steep learning curve to get started. YARN provides very low level API and developer has to learn 3 different protocols and write lots of code just to get an app up and running . The hello world app in yarn is distributed shell , it's ~2000 lines of code, a big burden for developers to learn and use yarn .

Many distributed applications have common needs such as application life cycle management, distributed process coordination and resiliency to failure. Unfortunately Yarn doesn't provide any support to tackle these issues and it's the developer responsibility to do so .

## Why Easy Yarn?

Easy yarn comes to play to complement the points where Yarn fails short . It provide a simpler API and distributed building blocks for developer, manages application life cycle and restart and reallocates tasks in case of failure transparently.  We believe Yarn is a great platform to develop distributed apps and easy yarn makes this much less painful and more enjoyable for all yarn developers.

## Features 
* Make use of Yarn resource allocation capabilities, hiding the its complexty 
* Application life cycle management , and automatic container faliure recovery  
* Simpler API programming constructs, abstracting yarn complexity:
  * TaskLets to implement individual work units
  * Phase for parallel Task execution
  * Phase Listeners for handling phase events 
  * Phase managers for common scenarios
  * Custom phase managers for complete control
  * Custom Application master
* Distributed programming constructs 
  * IPC via messaging between Tasklets : point to point or publisher subscribe  
  * Unified shared memory ( In memory)
  * Process synchronization mechanism (Zookeeper based):
      * locks ( read/write)
      * condition variables
      * semaphores






## Comparison with other projects
### Spring-Yarn
Spring yarn handles the lifecycle and allows user to use spring functionality,Unlike easy yarn it doesn't handle failures nor provides any distributed building blocks like messages passing or synchronization mechanisms. More over Spring yarn takes an opinionated decisions which might work at the beginning but won't be flexible enough to customize later. 

###Apache Twill ( Incubator)
Twill provides a programming model similar to threads over yarn. It overlaps with easy yarn in managing the life cycle and a thread like behavior . Easy yarn takes a different approach of providing a thread like behavior that's not dependent on runnable interface and provide a separate synchronization mechanism. if not ware enough Twill user might use java constructs which won't work in a distributed environment although same API (Threads) works fine for non distributed , confusing way of doing things we think.

### MPI on yarn ( mpich-yarn by Alibaba):

MPICH-yarn is an application running on Hadoop YARN that enables MPI programs running on Hadoop YARN clusters. 

The prospect of running Open MPI under YARN has been investigated by Ralph H. Castain of the Open MPI team. YARN was compared with the SLURM HPC scheduler as a platform for running Open MPI applications. The results heavily favored SLURM because of some of the fundamental design differences between the two approaches to cluster resource utilization.

Easy yarn doesn't aim to replace or implement MPI standard (~800+ page) on yarn. Instead by providing a simpler API and synchronization building blocks applications written for Yarn could be made simpler, High performance computing (HPC) might needs further adjustments to be yarn ready.

* references: 
  * https://github.com/alibaba/mpich2-yarn
  * http://www.admin-magazine.com/HPC/Articles/The-New-Hadoop
  * http://www.mpi-forum.org/docs/mpi-3.1/mpi31-report.pdf



## Sample programs 
### Helloworld 

``` 
public class HelloWorldTasklet extends Tasklet{	
	@Override
	public boolean start() {
		System.out.println("Hello world from HelloWorldTasklet!");
		return true;
	}
       @Override
	public boolean init(CommandLine commandLine) {		
		return true;
	}

	@Override
	public void setupOptions(Options opts) {	
		
	}
}```


### Distributed shell

Compare this with the distributed shell sample program provided by yarn , it's the hello world of yarn development (~2000 lines of code)

* references:
  * https://github.com/apache/hadoop-common/tree/trunk/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-applications/hadoop-yarn-applications-distributedshell





#Modern Enigma
Modern Enigma is a sample program 

- Encrypt command
 ``` 
yarn jar enigma-yarn-app-1.1.0.jar -jar /axp/gcp/cpsetlh/dev/enigma-yarn-app-1.1.0.jar -appMasterClass  com.tito.sampleapp.enigma.EnigmaAppMaster -enigmaCount 3 -enigmaTempDir /axp/gcp/cpsetlh/dev/test/enigma/enigmaTempDir -plainTextPath  /axp/gcp/cpsetlh/dev/test/enigma/plain/plain.txt -cipherTextPath /axp/gcp/cpsetlh/dev/test/enigma/cipher/cipher.text -keyPath /axp/gcp/cpsetlh/dev/test/enigma/EnigmaKey.key  -operation e  ```

- Decrypt command
  ```
yarn jar enigma-yarn-app-1.1.0.jar -jar /axp/gcp/cpsetlh/dev/enigma-yarn-app-1.1.0.jar -appMasterClass  com.tito.sampleapp.enigma.EnigmaAppMaster -enigmaTempDir /axp/gcp/cpsetlh/dev/test/enigma/enigmaTempDir -plainTextPath  /axp/gcp/cpsetlh/dev/test/enigma/plain/plain_decrypted.txt -cipherTextPath /axp/gcp/cpsetlh/dev/test/enigma/cipher/cipher.text  -keyPath /axp/gcp/cpsetlh/dev/test/enigma/EnigmaKey.key -operation d
  ```

