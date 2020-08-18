package com.ojas.weblog;
import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.ojas.weblog.util.ParamUtil;

import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
public class LogPreMapper extends Mapper<LongWritable,Text,NullWritable,Text>{
	private String log_pattern="^(\\S+) (\\S+) (\\S+) \\[(.+?)\\] \"([^\"]*)\" (\\S+) (\\S+) \"([^\"]*)\" \"([^\"]*)\"";
	int num_fields=9;
	Pattern pattern =null;
	MultipleOutputs  <NullWritable, Text> mos= null;
	protected void setup(Context context) {
		pattern=Pattern.compile(log_pattern);
		mos = new MultipleOutputs<NullWritable, Text>(context);
	}
	protected void cleanup(Context context) throws IOException, InterruptedException{
		mos.close();
	}
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
		String formattedStringValue=value.toString().replaceAll("\t"," ").trim();
	Matcher matcher=pattern.matcher(formattedStringValue);
	if(matcher.matches()&& num_fields==matcher.groupCount()) {
		String requestString=matcher.group(5);
		String separateReqCategory=getSeparateReqCategory(requestString);
		StringBuffer valueBuffer = new StringBuffer();
		valueBuffer.append(matcher.group(1)).append(ParamUtil.DELIMITER_TAB);					//remoteIP  14.97.118.184
		valueBuffer.append(matcher.group(2)).append(ParamUtil.DELIMITER_TAB);					//remotelogname
		valueBuffer.append(matcher.group(3)).append(ParamUtil.DELIMITER_TAB);					//user
		valueBuffer.append(matcher.group(4)).append(ParamUtil.DELIMITER_TAB);					//time
		valueBuffer.append(matcher.group(5)).append(ParamUtil.DELIMITER_TAB);					//requeststr
		
		valueBuffer.append(separateReqCategory).append(ParamUtil.DELIMITER_TAB);				//cat1 cat2 cat3 cat4 page param
		
		valueBuffer.append(matcher.group(6)).append(ParamUtil.DELIMITER_TAB);					//statuscode
		valueBuffer.append(matcher.group(7)).append(ParamUtil.DELIMITER_TAB);					//bytestring
		valueBuffer.append(matcher.group(8)).append(ParamUtil.DELIMITER_TAB);					//user-agent
		valueBuffer.append(matcher.group(9));													//referral
		
//		context.write(NullWritable.get(), new Text (valueBuffer.toString()));
		mos.write("ParsedRecords", NullWritable.get(), new Text (valueBuffer.toString()));
		
	}else {
		mos.write("BadRecords",NullWritable.get(),value);
	}
	}
	 private String getSeparateReqCategory(String requestString) {
		  String stringTokens[]=requestString.split(" ");
		  String separateReqCategory=null;
		  if(stringTokens.length==3) {
			  separateReqCategory=getProcessedRequest(stringTokens[1]);
		  }
		  else 
			  separateReqCategory=getProcessedDefaultRequest();
			  return separateReqCategory;
		  
	  }
	 private String getProcessedRequest(String request) {
		// StringBuffer paramStringBuffer=new StringBuffer();
		 StringBuffer separateReqCategoryBuffer=new StringBuffer();
		 String requestParamtokens[]=request.split("\\?");
		 String paramString="-";
		 boolean paramFlag=false;
		 if(requestParamtokens.length==2) {
			 paramString=requestParamtokens[1];
			 paramFlag=true;
		 }
		 else if (requestParamtokens.length > 2)										//? More than one time
			{
				paramFlag = true;
				StringBuffer paramStrBuff = new StringBuffer();
				for (int cnt = 1; cnt < requestParamtokens.length; cnt++)
				{
					paramStrBuff.append(requestParamtokens[cnt]);
					if (cnt < requestParamtokens.length - 1)
						paramStrBuff.append(ParamUtil.question);
				}
				paramString = paramStrBuff.toString();
			}
			
			String requestTokens [] = null;
			if (paramFlag)
				requestTokens = requestParamtokens[0].split("/");			//Request = /a/b/c	(case for /a/b/c?param)
			else
				requestTokens = request.split("/");							//Request = /a/b/c	(case for /a/b/c)
			
			
			int requestTokensLen = requestTokens.length;
			if (requestTokensLen == 0)													// for /
			{
				separateReqCategoryBuffer.append("/").append("	");
				separateReqCategoryBuffer.append(ParamUtil.DEFAULT_VALUE_DASH).append(ParamUtil.DELIMITER_TAB);
				separateReqCategoryBuffer.append(ParamUtil.DEFAULT_VALUE_DASH).append(ParamUtil.DELIMITER_TAB);
				separateReqCategoryBuffer.append(ParamUtil.DEFAULT_VALUE_DASH).append(ParamUtil.DELIMITER_TAB);
				separateReqCategoryBuffer.append(ParamUtil.DEFAULT_VALUE_DASH);
			}
			else if (requestTokensLen == 1)										//situation never come		(for a/)
			{
				separateReqCategoryBuffer.append(ParamUtil.DEFAULT_VALUE_DASH).append(ParamUtil.DELIMITER_TAB);
				separateReqCategoryBuffer.append(ParamUtil.DEFAULT_VALUE_DASH).append(ParamUtil.DELIMITER_TAB);
				separateReqCategoryBuffer.append(ParamUtil.DEFAULT_VALUE_DASH).append(ParamUtil.DELIMITER_TAB);
				separateReqCategoryBuffer.append(ParamUtil.DEFAULT_VALUE_DASH).append(ParamUtil.DELIMITER_TAB);
				separateReqCategoryBuffer.append(ParamUtil.DEFAULT_VALUE_DASH);
			}
			else if (requestTokensLen == 2)										// for /abc.html
			{
				separateReqCategoryBuffer.append(ParamUtil.DEFAULT_VALUE_DASH).append(ParamUtil.DELIMITER_TAB);
				separateReqCategoryBuffer.append(ParamUtil.DEFAULT_VALUE_DASH).append(ParamUtil.DELIMITER_TAB);
				separateReqCategoryBuffer.append(ParamUtil.DEFAULT_VALUE_DASH).append(ParamUtil.DELIMITER_TAB);
				separateReqCategoryBuffer.append(ParamUtil.DEFAULT_VALUE_DASH).append(ParamUtil.DELIMITER_TAB);
				separateReqCategoryBuffer.append(requestTokens[1]);
			}
			else if (requestTokensLen == 3)										//for /a/abc.html
			{
				separateReqCategoryBuffer.append(requestTokens[1]).append(ParamUtil.DELIMITER_TAB);
				separateReqCategoryBuffer.append(ParamUtil.DEFAULT_VALUE_DASH).append(ParamUtil.DELIMITER_TAB);
				separateReqCategoryBuffer.append(ParamUtil.DEFAULT_VALUE_DASH).append(ParamUtil.DELIMITER_TAB);
				separateReqCategoryBuffer.append(ParamUtil.DEFAULT_VALUE_DASH).append(ParamUtil.DELIMITER_TAB);
				separateReqCategoryBuffer.append(requestTokens[2]);
			}
			else if (requestTokensLen == 4)										// for /a/b/abc.html
			{
				separateReqCategoryBuffer.append(requestTokens[1]).append(ParamUtil.DELIMITER_TAB);
				separateReqCategoryBuffer.append(requestTokens[2]).append(ParamUtil.DELIMITER_TAB);
				separateReqCategoryBuffer.append(ParamUtil.DEFAULT_VALUE_DASH).append(ParamUtil.DELIMITER_TAB);
				separateReqCategoryBuffer.append(ParamUtil.DEFAULT_VALUE_DASH).append(ParamUtil.DELIMITER_TAB);
				separateReqCategoryBuffer.append(requestTokens[3]);
			}
			else if (requestTokensLen == 5)
			{
				separateReqCategoryBuffer.append(requestTokens[1]).append(ParamUtil.DELIMITER_TAB);
				separateReqCategoryBuffer.append(requestTokens[2]).append(ParamUtil.DELIMITER_TAB);
				separateReqCategoryBuffer.append(requestTokens[3]).append(ParamUtil.DELIMITER_TAB);
				separateReqCategoryBuffer.append(ParamUtil.DEFAULT_VALUE_DASH).append(ParamUtil.DELIMITER_TAB);
				separateReqCategoryBuffer.append(requestTokens[4]);
			}
			else if (requestTokensLen == 6)
			{
				separateReqCategoryBuffer.append(requestTokens[1]).append(ParamUtil.DELIMITER_TAB);
				separateReqCategoryBuffer.append(requestTokens[2]).append(ParamUtil.DELIMITER_TAB);
				separateReqCategoryBuffer.append(requestTokens[3]).append(ParamUtil.DELIMITER_TAB);
				separateReqCategoryBuffer.append(requestTokens[4]).append(ParamUtil.DELIMITER_TAB);
				separateReqCategoryBuffer.append(requestTokens[5]);
			}
			else if (requestTokensLen > 6)
			{
				separateReqCategoryBuffer.append(requestTokens[1]).append(ParamUtil.DELIMITER_TAB);
				separateReqCategoryBuffer.append(requestTokens[2]).append(ParamUtil.DELIMITER_TAB);
				separateReqCategoryBuffer.append(requestTokens[3]).append(ParamUtil.DELIMITER_TAB);
				StringBuffer requestTokensBuffer = new StringBuffer();
				for (int cnt = 4; cnt < requestTokensLen - 1; cnt++)
				{
					requestTokensBuffer.append(requestTokens[cnt]).append("/");
				}
				separateReqCategoryBuffer.append(requestTokensBuffer).append(ParamUtil.DELIMITER_TAB);
				separateReqCategoryBuffer.append(requestTokens[requestTokensLen - 1]);
			}
			
			
//			separateReqCategoryBuffer.append(requestStringTokens[2]).append(DELIMITER_TAB);
			separateReqCategoryBuffer.append(ParamUtil.DELIMITER_TAB).append(paramString);
			return separateReqCategoryBuffer.toString();
	 }
	 String getProcessedDefaultRequest()
		{
			StringBuffer separateReqCategoryBuffer = new StringBuffer();
			
			separateReqCategoryBuffer.append(ParamUtil.DEFAULT_VALUE_DASH).append(ParamUtil.DELIMITER_TAB);				//cat1
			separateReqCategoryBuffer.append(ParamUtil.DEFAULT_VALUE_DASH).append(ParamUtil.DELIMITER_TAB);				//cat2
			separateReqCategoryBuffer.append(ParamUtil.DEFAULT_VALUE_DASH).append(ParamUtil.DELIMITER_TAB);				//cat3
			separateReqCategoryBuffer.append(ParamUtil.DEFAULT_VALUE_DASH).append(ParamUtil.DELIMITER_TAB);				//cat4
			separateReqCategoryBuffer.append(ParamUtil.DEFAULT_VALUE_DASH).append(ParamUtil.DELIMITER_TAB);				//page
			
			separateReqCategoryBuffer.append(ParamUtil.DEFAULT_VALUE_DASH);										//param

			return separateReqCategoryBuffer.toString();
		}
}
 
