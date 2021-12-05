package orm.action;

public abstract class OrmAction {
    protected final String sql;
    protected final Object[] params;

    protected OrmAction(String sql, Object[] params) {
        this.sql = sql;
        this.params = params;
    }

    public String getSql() {
        return sql;
    }

    public Object[] getParams() {
        return params;
    }
}
