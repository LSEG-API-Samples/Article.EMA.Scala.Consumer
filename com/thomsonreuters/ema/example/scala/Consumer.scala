package com.thomsonreuters.ema.example.scala

import com.thomsonreuters.ema.access.Msg
import com.thomsonreuters.ema.access.AckMsg
import com.thomsonreuters.ema.access.GenericMsg
import com.thomsonreuters.ema.access.OmmArray
import com.thomsonreuters.ema.access.RefreshMsg
import com.thomsonreuters.ema.access.StatusMsg
import com.thomsonreuters.ema.access.UpdateMsg
import com.thomsonreuters.ema.access.Data
import com.thomsonreuters.ema.access.DataType;
import com.thomsonreuters.ema.access.DataType.DataTypes
import com.thomsonreuters.ema.access.EmaFactory
import com.thomsonreuters.ema.access.FieldEntry
import com.thomsonreuters.ema.access.FieldList
import com.thomsonreuters.ema.access.OmmConsumer
import com.thomsonreuters.ema.access.OmmConsumerClient
import com.thomsonreuters.ema.access.OmmConsumerEvent
import com.thomsonreuters.ema.access.OmmException
import com.thomsonreuters.ema.access.ElementList
import com.thomsonreuters.ema.rdm.EmaRdm
import com.thomsonreuters.ema.access.OmmConsumerConfig;
import com.thomsonreuters.ema.access.ReqMsg;
import com.thomsonreuters.ema.access.ElementList;
import com.thomsonreuters.ema.access.OmmArray;
import com.thomsonreuters.ema.access.EmaUtility;

//the mutable Set for itemNames(the List of RICs)
import scala.collection.mutable.Set;

class AppClient extends OmmConsumerClient {
  //This callback is invoked upon receiving a refresh message.
  //This message contains all currently available information about the item.
  //It also contains all fields or requested fields(in case of view request).
  def onRefreshMsg(refreshMsg: RefreshMsg,event:  OmmConsumerEvent) { 
      println("Refresh Message of: " +
             (if (refreshMsg.hasName()) refreshMsg.name()
              else "<not set>"))
     
      println("Domain Type: "+Consumer.domainTypeMap(refreshMsg.domainType))
     
      println("Service Name: " +
           (if (refreshMsg.hasServiceName()) refreshMsg.serviceName()
            else "<not set>"))
      
      println("Item State: " + refreshMsg.state());
      //Normally, it is data of Market Price domain 
      if (DataType.DataTypes.FIELD_LIST == refreshMsg.payload().dataType())
			  decode(refreshMsg.payload().fieldList());
      //for data of other domain types i.e. MARKET_BY_ORDER, MARKET_BY_PRICE, MARKET_MAKER and SYMBOL_LIST
      else if (DataType.DataTypes.MAP == refreshMsg.payload().dataType())
			  decode(refreshMsg.payload().map());
      
      println()
  }
  //This callback is invoked upon receiving an update message.
  //This message conveys any changes to an item’s data.
  def onUpdateMsg(updateMsg: UpdateMsg,event:  OmmConsumerEvent) {
      println("Update Message of: " +
             (if (updateMsg.hasName()) updateMsg.name()
              else "<not set>"))
     
      println("Domain Type: "+Consumer.domainTypeMap(updateMsg.domainType))
      
      println("Service Name: " +
           (if (updateMsg.hasServiceName()) updateMsg.serviceName()
            else "<not set>"))
            
      //Normally, it is data of Market Price domain 
      if (DataType.DataTypes.FIELD_LIST == updateMsg.payload().dataType())
			  decode(updateMsg.payload().fieldList());
      //for data of other domain types i.e. MARKET_BY_ORDER, MARKET_BY_PRICE, MARKET_MAKER and SYMBOL_LIST
		  else if (DataType.DataTypes.MAP == updateMsg.payload().dataType())
			  decode(updateMsg.payload().map());
      
      println()
  }
  //This callback is invoked upon receiving a status message.
  //This message conveys state change information associated with an item stream.
  def onStatusMsg(statusMsg: StatusMsg,event:  OmmConsumerEvent) {
      println("Status Message of:" +
            (if (statusMsg.hasName()) statusMsg.name()
            else "<not set>"))
      
      println("Domain Type: "+Consumer.domainTypeMap(statusMsg.domainType()))
       
      println("Service Name: " +
          (if (statusMsg.hasServiceName()) statusMsg.serviceName()
          else "<not set>"))
    
      if (statusMsg.hasState()) 
        println("Item State: " + statusMsg.state())
      println()
  }
  def onGenericMsg(genericMsg: GenericMsg,event:  OmmConsumerEvent) { }
  def onAckMsg(ackMsg: AckMsg,event:  OmmConsumerEvent) { }
  def onAllMsg(msg: Msg,event:  OmmConsumerEvent) { }
  
  //decode the field list according to each field's type
  def decode(fieldList: FieldList){
    fieldList.forEach( fieldEntry => {
        print("\tFid: " + fieldEntry.fieldId() + " Name: " + fieldEntry.name() + " DataType: " + DataType.asString(fieldEntry.load().dataType()) + " Value: ")
        if (Data.DataCode.BLANK == fieldEntry.code()) println(" blank")
				else {
				   fieldEntry.loadType() match {
				     case DataTypes.REAL => println(fieldEntry.real().asDouble)
				     case DataTypes.DATE => println(fieldEntry.date().day() + " / " + fieldEntry.date().month() + " / " + fieldEntry.date().year())
             case DataTypes.TIME => println(fieldEntry.time().hour() + ":" + fieldEntry.time().minute() + ":" + fieldEntry.time().second() + ":" + fieldEntry.time().millisecond())
             case DataTypes.DATETIME => println(fieldEntry.dateTime().day() + " / " + fieldEntry.dateTime().month() + " / " 
                                        + fieldEntry.dateTime().year() + "." + fieldEntry.dateTime().hour() + ":" 
                                        + fieldEntry.dateTime().minute() + ":" + fieldEntry.dateTime().second() + ":" 
                                        + fieldEntry.dateTime().millisecond() + ":" + fieldEntry.dateTime().microsecond()+ ":" 
                                        + fieldEntry.dateTime().nanosecond())
              case DataTypes.INT => println(fieldEntry.intValue())
              case DataTypes.UINT => println(fieldEntry.uintValue())
              case DataTypes.ASCII => println(fieldEntry.ascii())
              case DataTypes.ENUM =>  println(
                                        if (fieldEntry.hasEnumDisplay()) fieldEntry.enumDisplay()
                                        else fieldEntry.enumValue())
              case DataTypes.RMTES => println(fieldEntry.rmtes())
              case DataTypes.ERROR => println("(" + fieldEntry.error().errorCodeAsString() + ")");
              case _ => println()
				   }
				}
    }    
    )
  }
  //decode a map which can contain a summary data and the map entries
  def decode(map: com.thomsonreuters.ema.access.Map){
      if (DataTypes.FIELD_LIST == map.summaryData().dataType())
		  {
			  println("Map Summary Data:");
			  decode(map.summaryData().fieldList())
		  }
		  map.forEach(mapEntry => {
		    print("Map Entry: action = " + mapEntry.mapActionAsString() + " key = ")
		    mapEntry.key().dataType() match {
		      case DataTypes.BUFFER => println(EmaUtility.asAsciiString(mapEntry.key().buffer()))
		      case DataTypes.ASCII  => println(mapEntry.key().ascii())   
		      case DataTypes.RMTES  => println(mapEntry.key().rmtes());
		    }
		    if (DataTypes.FIELD_LIST == mapEntry.loadType())
			  {
				  println("Map Entry Data:");
				  decode(mapEntry.fieldList());
			  }
		  }
		  )
   }
}
object Consumer {
  //Set each mutable field/variable to use the default value
  var server: String = "localhost:14002"
  var service: String = "DIRECT_FEED"
  var user: String = "user"
  var domainType: Int = EmaRdm.MMT_MARKET_PRICE
  var itemNames: scala.collection.mutable.Set[String] = Set("IBM.N") 
  //An empty set represents all fields
  var fieldIds: scala.collection.mutable.Set[Int] = Set() 
  var streamingReq: Boolean = true
  var runTime: Int = 60
  // List of Domain Types
  val domainTypeMap = Map(EmaRdm.MMT_MARKET_PRICE -> "MARKET_PRICE" ,  EmaRdm.MMT_MARKET_BY_ORDER->"MARKET_BY_ORDER" , EmaRdm.MMT_MARKET_BY_PRICE ->  "MARKET_BY_PRICE", EmaRdm.MMT_MARKET_MAKER->"MARKET_MAKER", EmaRdm.MMT_SYMBOL_LIST->"SYMBOL_LIST" )
  
 //Utility method show application help message.
  def showHelp() {
    val help: String =  
        "command option list:\n" +
        "    -server <server_host_port>           The Server host and port. The default is localhost:14002.\n" +
        "    -serviceName <service_name>          The Service name. The default is DIRECT_FEED.\n" +
        "    -username <name>                     The Name of application user. The default is user.\n" +
        "    -domainType <domain_type>            The Domain Type of the requested item(s). The default is MARKET_PRICE.\n" +
        "                                         The valid Domain Types supported by this application are:\n" +
        "                                         MARKET_PRICE, MARKET_BY_ORDER, MARKET_BY_PRICE, MARKET_MAKER and SYMBOL_LIST.\n" +
        "    -itemNames <list_of_RICs>            The List of RICs separated by ','. The default is IBM.N.\n" +
        "    -fieldIds <list_of_FieldIds>         The List of field Ids separated by ','. The default is all fields.\n" +
        "    -streamingReq <true or false>        Are all streaming requests?. If it is true, the application receives a Refresh follow by Update messages.\n" + 
        "                                         Otherwise, only a Refresh is received. The default is true.\n" +
        "    -runTime <seconds>                   How long (in seconds) application should run before exiting. The default is 60.\n"

        println(help)
        System.exit(1)
  }
  
  //Extract program parameters from the command line.
  def getCommandLineOptions(args:Array[String]) {
    try {
      var i: Int = 0
      while( i < args.length) { 
        if (args(i).equalsIgnoreCase("-server")) {
          i += 1
          server = args(i)
        }
        else if (args(i).equalsIgnoreCase("-service")) {
           i += 1
          service = args(i)
        }
        else if (args(i).equalsIgnoreCase("-user")) {
           i += 1
           user = args(i)
        }
        else if (args(i).equalsIgnoreCase("-domainType")) {
           i += 1
           var isValidDomainType: Boolean = false
           for ((domainId,domainName) <- domainTypeMap) {
              if(args(i).toUpperCase().equals(domainName) && !isValidDomainType) {
                domainType = domainId
                isValidDomainType = true
              }
          } 
           if(!isValidDomainType) {
              //if it is invalid domain type, print the valid types then exits.
              println("Error: The domainType " + args(i) + " is not supported by this application. The valid domainType are:");
              for ((domainId,domainName) <- domainTypeMap) {
                print(domainName + ", ")
             }
             println()
             System.exit(1);
          }
        }
        else if (args(i).equalsIgnoreCase("-itemNames")) {
           i += 1
           val tmpItems: Array[String] = args(i).split(",")
           itemNames.remove("IBM.N")
           for ( anItem <- tmpItems ) {
               itemNames.add(anItem)
            }
        }
        else if (args(i).equalsIgnoreCase("-fieldIds")) {
           i += 1
           val tmpFids: Array[String] = args(i).split(",")     
           for ( anFid <- tmpFids ) {
             try {
                 fieldIds.add(anFid.toInt)
             } 
             catch  {
                case e: NumberFormatException => {
                  println("Warning: Field Id " + anFid + " is not a number. Ignore it.")
                }
             }
           }
        }
        else if (args(i).equalsIgnoreCase("-streamingReq")) {
           i += 1
           val tmpReq: String = args(i);
           if(args(i).equalsIgnoreCase("false")) 
             streamingReq = false
        }
        else if (args(i).equalsIgnoreCase("-runTime")) {
           i += 1
           try {
             if((args(i).toInt) < 0) {
               println("Warning: The runTime " + args(i) + " is invalid so the default, 60 seconds, is used.")
             } else {
               runTime = args(i).toInt
             }
           }
           catch  {
              case e: NumberFormatException => {
                 println("Warning: The runTime " + args(i) + " is invalid so the default, 60 seconds, is used.")
             }
           }
        } 
        i += 1;
      }
    }
    catch  {
      case e: ArrayIndexOutOfBoundsException => {
           println("Error: Invalid program parameter(s)");
           showHelp();
      }
    }
  }
  //create batch and/or view request
  def createBatchViewElementList(items:Set[String],fidsSet:Set[Int]) : ElementList = {
      val batchview: ElementList = EmaFactory.createElementList()
      
      //If there are more than 1 items/RICs,
      //create a batch request by add each item(RIC) into an array of the ElementList
      if(items.size > 1) { 
         val itemsarray: OmmArray = EmaFactory.createOmmArray()
         items.foreach(anItem =>
           itemsarray.add(EmaFactory.createOmmArrayEntry().ascii(anItem))
         )
         batchview.add(EmaFactory.createElementEntry().array(EmaRdm.ENAME_BATCH_ITEM_LIST, itemsarray));
      }
      
      //If field ids are specified,
      //create a view request by adding each field id into an array of the ElementList
      //set view type is 1 to indicate that the array contains field ids.
      if(fidsSet.isEmpty == false) 
      { 
        val fidsarray: OmmArray = EmaFactory.createOmmArray()
        fidsSet.foreach(afid =>
           fidsarray.add(EmaFactory.createOmmArrayEntry().intValue(afid))
        )
        batchview.add(EmaFactory.createElementEntry().uintValue(EmaRdm.ENAME_VIEW_TYPE, 1))
        batchview.add(EmaFactory.createElementEntry().array(EmaRdm.ENAME_VIEW_DATA, fidsarray))
      }
      
      return batchview         
  }
  //Show all parameters used by the application
  def showAllParameters() {
     println("The application is using the following parameters:");
     println("server="+server)
     println("service="+service)
     println("user="+user)
     println("domainType="+domainTypeMap(domainType))
     println("itemNames="+itemNames.mkString(","))
     print("fieldIds=");
       if (fieldIds.isEmpty) println("all fields")
       else println(fieldIds.mkString(","))
       
     println("streamingReq="+streamingReq)
     println("runTime="+runTime)
     println();
  }
  def main (args:Array[String]) {
     var myapp = new AppClient
 
     //Extract program parameters from the command line.
     this.getCommandLineOptions(args)
     
     //Show all parameters used by the application
     showAllParameters() 
     
     //EMA part to subscribe items and retrieve data
     var consumer: OmmConsumer = null
     try {
        val appClient: AppClient = new AppClient()
        val config: OmmConsumerConfig = EmaFactory.createOmmConsumerConfig()
        //Create EMA OmmConsumer object. 
        //The OmmConsumer connects and logs in to the server then
        //it obtains services information, loads or downloads dictionary information automatically.
        consumer = EmaFactory.createOmmConsumer(config.host(server).username(user))
        val reqMsg: ReqMsg = EmaFactory.createReqMsg()
        //1 item/RIC with all fields so batch and view request are not created; specify the item/RIC in the name(..) method
        if(itemNames.size == 1 && fieldIds.isEmpty) { 
            //send the request 
            consumer.registerClient(reqMsg.serviceName(service).domainType(domainType).name(itemNames.head).interestAfterRefresh(streamingReq), appClient)
        }
        //1 or more items/RICs with all fields or specified fields 
        else { 
            //create batch and/or view request
            val batchview: ElementList = createBatchViewElementList(itemNames,fieldIds)
            //send view request for 1 item and specify the item/RIC in the name(..) method
            if(itemNames.size == 1)
              consumer.registerClient(reqMsg.serviceName(service).domainType(domainType).name(itemNames.head).interestAfterRefresh(streamingReq).payload(batchview), appClient)
            //send batch(multiple items) and/or view request
            else 
              consumer.registerClient(reqMsg.serviceName(service).domainType(domainType).interestAfterRefresh(streamingReq).payload(batchview), appClient)
       }
        //EMA calls onRefreshMsg(), onUpdateMsg() and onStatusMsg() when an event is received automatically
        //till the runTime seconds elapsed.
        Thread.sleep(runTime * 1000)
     } catch {
        case i: InterruptedException => {
          println("InterruptedException:" + i.getMessage)
        }
        case o : OmmException => {
          println("OmmException:" + o.getMessage)
        }
     } finally {
        //before the application exits, log out and disconnect from the server, 
        //at which time all open item streams are closed.
        if (consumer != null) 
          consumer.uninitialize()
        System.exit(0);
    }
  }
}