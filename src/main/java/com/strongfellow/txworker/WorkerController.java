package com.strongfellow.txworker;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.strongfellow.utils.BlockParser;
import com.strongfellow.utils.Utils;
import com.strongfellow.utils.data.TXout;
import com.strongfellow.utils.data.Transaction;

@RestController
public class WorkerController {

	@ResponseStatus(value=HttpStatus.INTERNAL_SERVER_ERROR, reason="Service Error")
    public class WorkerException extends RuntimeException {
		private static final long serialVersionUID = 1L;

		public WorkerException(Throwable t) {
			super(t);
		}
		
    }
	
	public WorkerController() {
		AmazonDynamoDBClient amazon = new AmazonDynamoDBClient();
		amazon.setRegion(Region.getRegion(Regions.US_WEST_2));
		this.dynamo = new DynamoDB(amazon);
	}
	
	private final DynamoDB dynamo;
	private final AmazonS3 s3 = new AmazonS3Client();
	private static final Logger logger = LoggerFactory.getLogger(WorkerController.class);
	private final BlockParser blockParser = new BlockParser();
	
	@RequestMapping(method=RequestMethod.POST, value="tx", consumes="application/json")
	@ResponseBody
	public String handle(@RequestBody TXMessage message) throws Exception {
		try {
			logger.info("handling tx {}", message.hash);
			String key = String.format("transactions/%s/payload", message.hash);
			S3Object obj = s3.getObject("strongfellow.com", key);
			InputStream in = obj.getObjectContent();
			byte[] bytes = IOUtils.toByteArray(in);
			Transaction t = this.blockParser.parseTransaction(bytes, 0);
			BatchWriteItemRequest batch = new BatchWriteItemRequest();
			List<WriteRequest> writeRequests =  new ArrayList<WriteRequest>();
			String txHash = t.getTxHash();
			byte[] tx = Utils.unhex(txHash);
			int i = 0;
			TableWriteItems items = new TableWriteItems("txouts");
			for (TXout out : t.getTxOuts()) {
				PrimaryKey primaryKey = new PrimaryKey("hash", tx, "index", i++);
				Item item = new Item().withPrimaryKey(primaryKey)
									.withNumber("value", out.getValue())
									.withString("script", out.getLockingScript());
				items.addItemToPut(item);
				if (i == 25) {
					this.dynamo.batchWriteItem(items);
					i = 0;
					items = new TableWriteItems("t`xouts");
					logger.info("we wrote in here");
				}
			}
			if (i > 0) {
				this.dynamo.batchWriteItem(items);
				logger.info("we wrote out here");
			}
			logger.info("we are golden");
			return "";
		} catch(Throwable t) {
			throw new WorkerException(t);
		}
//		in.close();
//		return "";
	}
}
