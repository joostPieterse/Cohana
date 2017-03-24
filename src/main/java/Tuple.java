
public class Tuple {
    int user;
    String action;
    long time;
    String role;
    String country;
    int gold;

    public Tuple(int user, String action, long time, String role, String country, int gold) {
        this.user = user;
        this.action = action;
        this.time = time;
        this.role = role;
        this.country = country;
        this.gold = gold;
    }

    @Override
    public String toString() {
        return "{user:" + user + " action:" + action + " time:" + time + " role:" + role + " country:" + country + " gold:" + gold+"}";
    }
}
