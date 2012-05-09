package com.dbdeploy.database.changelog;

import com.dbdeploy.AppliedChangesProvider;
import com.dbdeploy.exceptions.SchemaVersionTrackingException;
import com.dbdeploy.scripts.ChangeScript;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * This class is responsible for all interaction with the changelog table
 */
public class DatabaseSchemaVersionManager implements AppliedChangesProvider {

    private final QueryExecuter queryExecuter;
    private final String changeLogTableName;
    private final String allowMissingChangeLog;
    private CurrentTimeProvider timeProvider = new CurrentTimeProvider();

    public DatabaseSchemaVersionManager(QueryExecuter queryExecuter, String changeLogTableName, String allowMissingChangeLog) {
        this.queryExecuter = queryExecuter;
        this.changeLogTableName = changeLogTableName;
        this.allowMissingChangeLog = allowMissingChangeLog;
    }
    public DatabaseSchemaVersionManager(QueryExecuter queryExecuter, String changeLogTableName) {
        this.queryExecuter = queryExecuter;
        this.changeLogTableName = changeLogTableName;
        this.allowMissingChangeLog = "false";
    }

	public List<Long> getAppliedChanges() {
		try {
			ResultSet rs = queryExecuter.executeQuery(
					"SELECT change_number FROM " + changeLogTableName + "  ORDER BY change_number");

			List<Long> changeNumbers = new ArrayList<Long>();

			while (rs.next()) {
				changeNumbers.add(rs.getLong(1));
			}

			rs.close();

			return changeNumbers;
		} catch (SQLException e) {
			if (this.allowMissingChangeLog!=null && this.allowMissingChangeLog.toUpperCase().equals("TRUE"))
			{
				return null;
			}
			else 
			{
				throw new SchemaVersionTrackingException("Could not retrieve change log from database because: "
						+ e.getMessage(), e);
			}
			
		}
	}

    public String getChangelogDeleteSql(ChangeScript script) {
		return String.format(
			"DELETE FROM " + changeLogTableName + " WHERE change_number = %d",
				script.getId());
	}

    public void recordScriptApplied(ChangeScript script) {
        try {
            queryExecuter.execute(
                    "INSERT INTO " + changeLogTableName + " (change_number, complete_dt, applied_by, description)" +
                            " VALUES (?, ?, ?, ?)",
                    script.getId(),
                    new Timestamp(timeProvider.now().getTime()),
                    queryExecuter.getDatabaseUsername(),
                    script.getDescription()
                    );
        } catch (SQLException e) {
            throw new SchemaVersionTrackingException("Could not update change log because: "
                    + e.getMessage(), e);
        }
    }

    public void setTimeProvider(CurrentTimeProvider timeProvider) {
        this.timeProvider = timeProvider;
    }

    public static class CurrentTimeProvider {

        public Date now() {
            return new Date();
        }
    }
}
