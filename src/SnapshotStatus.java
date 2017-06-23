import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;

/**
 * Created by faizan on 11/4/16.
 */
public class SnapshotStatus {
    int snapshotID;

    LocalSnapshot lSnapshot = null;
    HashMap<String, String> channelStatus = null;
    TreeMap<String, Integer> channelBalance = null;

    public SnapshotStatus(){
        lSnapshot= new LocalSnapshot();
        lSnapshot.messages=new ArrayList<>();
        channelBalance = new TreeMap<>();
        channelStatus = new HashMap<>();
    }
}
