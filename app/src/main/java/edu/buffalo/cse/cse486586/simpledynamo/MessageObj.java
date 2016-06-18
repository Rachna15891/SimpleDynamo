package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by rachna on 3/28/16.
 */
public class MessageObj implements Serializable {

    String msgType= null;
    String senderNodeID = null;
    ArrayList<String> ListOfAliveNodes = new ArrayList<String>();
    private HashMap mContentValues = new HashMap();
    String ReceiverForMsg = null;
    String successor1 = null;
    String sucessor2 = null;
    String queryFile = null;
    String whoSentReplyAll = null;

    public String getMsgType() {
        return msgType;
    }

    public void setMsgType(String msgType) {
        this.msgType = msgType;
    }

    public String getSenderNodeID() {
        return senderNodeID;
    }

    public void setSenderNodeID(String senderNodeID) {
        this.senderNodeID = senderNodeID;
    }

    public ArrayList<String> getListOfAliveNodes() {
        return ListOfAliveNodes;
    }

    public void setListOfAliveNodes(ArrayList<String> listOfAliveNodes) {
        ListOfAliveNodes = listOfAliveNodes;
    }

    public String getSuccessor1() {
        return successor1;
    }

    public void setSuccessor1(String successor1) {
        this.successor1 = successor1;
    }

    public String getSucessor2() {
        return sucessor2;
    }

    public void setSucessor2(String sucessor2) {
        this.sucessor2 = sucessor2;
    }

    public String getReceiverForMsg() {
        return ReceiverForMsg;
    }

    public void setReceiverForMsg(String receiverForMsg) {
        ReceiverForMsg = receiverForMsg;
    }

    public HashMap getmContentValues() {
        return mContentValues;
    }
    public void setmContentValues(String key, String value) {
        mContentValues.put("key",key);
        mContentValues.put("value",value);
    }
    public void setmContentValues(HashMap<String,String > map) {
        mContentValues = map;
    }

    public String getWhoSentReplyAll() {
        return whoSentReplyAll;
    }

    public void setWhoSentReplyAll(String whoSentReplyAll) {
        this.whoSentReplyAll = whoSentReplyAll;
    }

    public String getQueryFile() {
        return queryFile;
    }

    public void setQueryFile(String queryFile) {
        this.queryFile = queryFile;
    }

    @Override
    public String toString() {
        return "MessageObj{" +
                "msgType='" + msgType + '\'' +
                ", senderNodeID='" + senderNodeID + '\'' +
                ", ListOfAliveNodes=" + ListOfAliveNodes +
                ", mContentValues=" + mContentValues +
                ", ReceiverForMsg='" + ReceiverForMsg + '\'' +
                ", successor1='" + successor1 + '\'' +
                ", sucessor2='" + sucessor2 + '\'' +
                ", queryFile='" + queryFile + '\'' +
                ", whoSentReplyAll='" + whoSentReplyAll + '\'' +
                '}';
    }
}
