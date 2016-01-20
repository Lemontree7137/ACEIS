
package org.insight_centre.citypulse.commons.data.json;

import java.util.Date;
/*
 * @author: Thu-Le Pham
 * this class defines an event
 */
public class ContextualEvent implements Cloneable{
	
	private String eventID;
	private Location eventLocation;
	private String eventName;
	private Date eventTime; 
	private String eventValue;
	private int weight;
	
	public ContextualEvent(){
		
	} 
		
	public ContextualEvent(String id, String name, String value, Date time, Location location) {
		// TODO Auto-generated constructor stub
		this.setEvent(id, name, value, time, location);	
	}
	
	public ContextualEvent(ContextualEvent e){
		setEvent(e.getEventID(), e.getEventName(), e.getEventValue(), e.getEventTime(), e.getEventLocation());
		this.weight = e.weight;
	}

	@Override
	public ContextualEvent clone() throws CloneNotSupportedException {
		// TODO Auto-generated method stub
		return (ContextualEvent)super.clone();
	}
	
	public String getEventID(){
		return eventID;
	}
	
	public Location getEventLocation(){
		return this.eventLocation;
	}
	
	public String getEventName(){
		return eventName;
	}
	
	public Date getEventTime(){
		return eventTime;
	}
	
	public String getEventValue(){
		return eventValue;
	}
	
	public int getEventWeight(){
		return this.weight;
	}
	
	public void setEvent(String id, String name, String value, Date time, Location location ){
		this.eventID = id;
		this.eventName = name;
		this.eventValue = value;
		this.eventTime = (Date) time.clone(); 
		this.eventLocation = location;
		this.weight  = -1;
	}
	
	public void setEventLocation(Location l){
		this.eventLocation = l;
	}
	
	public void setEventID(String id){
		this.eventID = id;
	}
	
	public void setEventName(String name){
		this.eventName = name;
	}
	
	public void setEventTime(Date time){
		this.eventTime = time;
	}
	
	public void setEventValue(String value){
		this.eventValue = value;
	}
	
	public void setEventWeight(int w){
		this.weight = w;
	}

	@Override
	public String toString(){
		//DateFormat formatter = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");
		//String time = "time_" + formatter.format(this.getEventTime());
		String t = "Event[id = " + this.eventID + ", name = " + this.eventName + ", value = " + this.eventValue +", time = " + this.eventTime +", location = " + this.eventLocation.toString() + ", weight = " + this.weight + "]\n ";
		return t;
	}

}
