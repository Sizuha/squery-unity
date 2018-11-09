using System;
using System.Data;
using System.Collections.Generic;
using Mono.Data.Sqlite;
using UnityEngine;
using System.Globalization;

namespace squery {

	public interface ISQueryRow {
		void ParseFromDB(SqliteDataReader reader);
		Dictionary<string, object> ToValues();
	}

	public class SQuery {
		readonly string dbPath;

		public SQuery(string dbWithPath) {
			this.dbPath = dbWithPath;
		}

		public TableQuery From(string table) {
			var result = new TableQuery(this, table);
			return result;
		}

		public SqliteConnection Open() {
			Debug.Log("try open db: " + dbPath);

			//var conn = new SqliteConnection("URI=file:" + dbPath);
			var conn = new SqliteConnection("Data Source=" + dbPath + ";Version=3;");
			conn.Open();

			return conn;
		}

		public static List<string> ExtractParamers(string queryStr) {
			var result = new List<string>();
			bool inTag = false;
			var nameBuffer = new FastString();

			foreach (var s in queryStr) {
				if (s == '@') {
					inTag = true;
					nameBuffer.Clear();
					continue;
				}

				if (inTag) {
					if (s >= 'A' && s <= 'z' || s == '_' || s >= '0' && s <= '9') {
						nameBuffer.Append(s);
					}
					else {
						if (!nameBuffer.IsEmpty()) {
							result.Add(nameBuffer.ToString());
							nameBuffer.Clear();
						}
						inTag = false;
					}
				}
			}

			return result;
		}

		static SqliteCommand CreateCommand(SqliteConnection conn, string rawQuery, object[] args) {
			var cmd = conn.CreateCommand();
			cmd.CommandType = CommandType.Text;
			cmd.CommandText = rawQuery;

			Debug.Log("CreateCommand query=" + rawQuery);
			//Debug.Log("CreateCommand arg cnt=" + args.Length);
			//int i = 0;
			//foreach (var a in args) {
			//	Debug.Log("arg" + (i++) + ": " + a);
			//}

			// for ? style parameters
			/*foreach (var arg in args) {
				SqliteParameter p;

				if (arg is Int16 || arg is short)
					p = new SqliteParameter(DbType.Int16, arg);
				else if (arg is Int32 || arg is int)
					p = new SqliteParameter(DbType.Int32, arg);
				else if (arg is Int64 || arg is long)
					p = new SqliteParameter(DbType.Int64, arg);
				else if (arg is float || arg is double)
					p = new SqliteParameter(DbType.Double, arg);
				else if (arg is sbyte)
					p = new SqliteParameter(DbType.SByte, arg);
				else if (arg is byte)
					p = new SqliteParameter(DbType.Byte, arg);
				else if (arg is bool)
					p = new SqliteParameter(DbType.Boolean, arg);
				else 
					p = new SqliteParameter(DbType.String, arg.ToString());

				cmd.Parameters.Add(p);
			}*/

			// for @name style parameters
			var paramNames = ExtractParamers(rawQuery);
			int idx = 0;
			foreach (var p in paramNames) {
				if (!cmd.Parameters.Contains(p)) {
					var value = args[idx++];
					cmd.Parameters.Add(new SqliteParameter {
						ParameterName = p,
						Value = value
					});

					Debug.Log("@" + p + ": " + value);
				}
			}

			return cmd;
		}

		public int ExecuteNonQuery(string rawQuery, params object[] args) {
			using (var conn = Open()) {
				using (var cmd = CreateCommand(conn, rawQuery, args)) {
					cmd.Prepare();
					return cmd.ExecuteNonQuery();
				}
			}
		}

		public object ExecuteScalar(string rawQuery, params object[] args) {
			using (var conn = Open()) {
				using (var cmd = CreateCommand(conn, rawQuery, args)) {
					cmd.Prepare();
					return cmd.ExecuteScalar();
				}
			}
		}

		public class QueryResult : IDisposable {
			public QueryResult(SqliteConnection connection, SqliteDataReader reader) {
				this.connection = connection;
				this.reader = reader;
			}

			public SqliteConnection connection;
			public SqliteDataReader reader;

			public void Close() {
				if (!reader.IsClosed) reader.Close();
				connection.Close();
			}

			public void Dispose() {
				Close();
			}
		}

		public QueryResult ExecuteQuery(string rawQuery, params object[] args) {
			var conn = Open();
			using (var cmd = CreateCommand(conn, rawQuery, args)) {
				cmd.Prepare();
				return new QueryResult(conn, cmd.ExecuteReader());
			}
		}

		public static void CopyFromStreamingAssets(string dbFile, string outPath = null, bool overwrite = false) {
			var sourcePath = System.IO.Path.Combine(Application.streamingAssetsPath, dbFile);
			var outputPath = string.IsNullOrEmpty(outPath)
								   ? System.IO.Path.Combine(Application.persistentDataPath, dbFile)
								   : outPath;

			//Debug.Log("CopyFromStreamingAssets from: " + sourcePath + " --> to: " + outputPath);

			if (sourcePath.Contains("://")) {
				if (overwrite || !System.IO.File.Exists(outputPath)) {
					var www = new WWW(sourcePath);
					while (!www.isDone) { }

					if (String.IsNullOrEmpty(www.error)) {
						try {
							System.IO.File.WriteAllBytes(outputPath, www.bytes);
						}
						catch (System.IO.IOException e) {
							Debug.LogError(e.ToString());
						}
					}
				}
			}
			else if (overwrite || !System.IO.File.Exists(outputPath)) {
				try {
					System.IO.File.Copy(sourcePath, outputPath, overwrite);
				}
				catch (System.IO.IOException e) {
					Debug.LogError(e.ToString());
				}
			}
		}

		public int GetUserVersion() {
			int result = Convert.ToInt32(ExecuteScalar("PRAGMA user_version;"));

			return result;
		}

		public bool NeedUpgrade(int targetVersion) {
			return GetUserVersion() < targetVersion;
		}

		public void SetUserVersion(int version) {
			ExecuteNonQuery("PRAGMA user_version=@ver;", version);
		}

		//--- Utils -----------------------------------------------

		public static string DateTimeToStr(DateTime datetime, bool withoutTime = false) {
			return datetime.ToString(withoutTime ? "yyyy-MM-dd" : "yyyy-MM-dd HH:mm:ss");
		}

		public static DateTime ParseDateTime(string formatted, DateTime whenError, bool withoutTime = false) {
			DateTime parsedDate;

			if (DateTime.TryParseExact(
				formatted,
				withoutTime ? "yyyy-MM-dd" : "yyyy-MM-dd HH:mm:ss",
				null,
				DateTimeStyles.None,
				out parsedDate)) {
				return parsedDate;
			}

			return whenError;
		}

		public static string EscapeLikeSpecialChrs(string source, char escapeChar) {
			var result = new FastString(source.Length * 2);
			var targets = new char[] { '%', '_' };

			foreach (var c in source) {
				foreach (var sc in targets) {
					if (c == sc) {
						result.Append(escapeChar);
						break;
					}
				}

				result.Append(c);
			}

			return result.ToString();
		}

	}

	public class TableQuery {
		const int CMD_INIT_BUFFER_SIZE = 128;

		readonly SQuery db;
		readonly string tableName;

		bool isDistict = false;

		readonly FastString whereStr = new FastString();
		readonly List<object> whereArgs = new List<object>();

		Dictionary<string, object> values = new Dictionary<string, object>();

		readonly FastString orderBy = new FastString();
		string groupBy = string.Empty;
		int limit;
		int limitOffset;

		readonly List<string> keys = new List<string>(3);


		public TableQuery(SQuery db, string tableName) {
			this.db = db;
			this.tableName = tableName;
		}

		public TableQuery Reset() {
			whereStr.Clear();
			whereArgs.Clear();

			values.Clear();
			orderBy.Clear();
			groupBy = string.Empty;
			limit = 0;
			limitOffset = 0;
			isDistict = false;
			return this;
		}

		public TableQuery Keys(params string[] keys) {
			this.keys.Clear();
			this.keys.AddRange(keys);
			return this;
		}

		public TableQuery Where(string where, params object[] args) {
			whereStr.Clear();
			whereStr.Append(where);

			whereArgs.Clear();
			whereArgs.AddRange(args);
			
			return this;
		}

		public TableQuery WhereAnd(string where, params object[] args) {
			if (whereStr.IsEmpty())
				return Where("("+where+")", args);

			whereStr.Append(" AND (");
			whereStr.Append(where);
			whereStr.Append(")");
			whereArgs.AddRange(args);
			return this;
		}

		public TableQuery Values(Dictionary<string, object> keyValues) {
			this.values = keyValues;
			return this;
		}

		public TableQuery OrderBy(string field, bool asc = true) {
			if (!orderBy.IsEmpty()) {
				orderBy.Append(", ");
			}
			orderBy.Append(field).Append(asc ? " ASC" : " DESC");

			return this;
		}

		public TableQuery SetOrderBy(string orderByStr) {
			orderBy.Clear();
			orderBy.Append(orderByStr);
			return this;
		}

		public TableQuery GroupBy(string groupByStr) {
			this.groupBy = groupByStr;
			return this;
		}

		public TableQuery Limit(int count, int offset = 0) {
			limit = count;
			limitOffset = offset;
			return this;
		}

		public TableQuery Distnict() {
			isDistict = true;
			return this;
		}

		public int Insert(params object[] values) {
			var cmd = new FastString(CMD_INIT_BUFFER_SIZE);
			cmd.Append("INSERT INTO ").Append(tableName).Append(" VALUES(");

			var vals = new List<string>(values.Length);
			bool first = true;
			int argCnt = 0;
			foreach (var v in values) {
				if (first) first = false; else cmd.Append(", ");
				//cmd.Append("?");
				cmd.Append("@insArg" + (++argCnt));
			}
			cmd.Append(");");

			return db.ExecuteNonQuery(cmd.ToString(), values);
		}

		public int Insert(ISQueryRow values) {
			Values(values.ToValues());
			return Insert();
		}

		public int Insert() {
			var cmd = new FastString(CMD_INIT_BUFFER_SIZE * 2);
			cmd.Append("INSERT INTO ").Append(tableName);

			var fieldStr = new FastString(CMD_INIT_BUFFER_SIZE / 2);
			var valueStr = new FastString(CMD_INIT_BUFFER_SIZE);

			bool first = true;
			int argCnt = 0;
			foreach (var kv in values) {
				if (first) {
					first = false;
				}
				else {
					fieldStr.Append(", ");
					valueStr.Append(", ");
				}

				fieldStr.Append(kv.Key);
				valueStr.Append("@insArg" + (++argCnt));
				//valueStr.Append("?");
			}

			cmd.Append("(").Append(fieldStr).Append(") VALUES(").Append(valueStr).Append(");");

			var valList = new List<object>(values.Count);
			valList.AddRange(values.Values);

			return db.ExecuteNonQuery(cmd.ToString(), valList.ToArray());
		}

		bool ContainKeys(string fieldName) {
			return keys.Contains(fieldName);
		}

		public int Update(ISQueryRow values) {
			Values(values.ToValues());
			SetWhereForCheckKey(this.values);
			return Update();
		}

		public int Update() {
			var cmd = new FastString(CMD_INIT_BUFFER_SIZE);
			cmd.Append("UPDATE ").Append(tableName).Append(" SET ");
			var args = new List<object>(values.Count + whereArgs.Count);

			bool first = true;
			int argCnt = 0;
			foreach (var kv in values) {
				if (ContainKeys(kv.Key)) continue;

				if (first) first = false; else cmd.Append(", ");
				//cmd.Append(kv.Key).Append("=?");
				cmd.Append(kv.Key).Append("=@upArg" + (++argCnt));
				args.Add(kv.Value);
			}

			if (!whereStr.IsEmpty()) {
				cmd.Append(" WHERE ").Append(whereStr);
				foreach (var a in whereArgs) {
					args.Add(a);
				}
			}

			cmd.Append(";");
			return db.ExecuteNonQuery(cmd.ToString(), args.ToArray());
		}

		public int InsertOrUpdate(ISQueryRow values) {
			Values(values.ToValues());
			return InsertOrUpdate();
		}

		public int InsertOrUpdate() {
			var result = -1;

			SetWhereForCheckKey(this.values);
			if (values != null && values.Count > 0 && !whereStr.IsEmpty()) {
				try {
					result = Insert();
				}
				catch (SqliteException e) {
					Debug.Log("Insert Failed. Try Update. " + e.GetHashCode());
					result = -1;
				}

				if (result < 1) {
					result = Update();
				}
			}
			else {
				throw new InvalidExpressionException("need WHERE");
			}

			return result;
		}

		public int Delete() {
			var cmd = new FastString(CMD_INIT_BUFFER_SIZE);
			cmd.Append("DELETE FROM ").Append(tableName);

			if (!whereStr.IsEmpty()) {
				cmd.Append(" WHERE ").Append(whereStr);
			}

			cmd.Append(";");

			return db.ExecuteNonQuery(cmd.ToString(), whereArgs.ToArray());
		}

		string CreateSelectQuery(bool isCount, params string[] columns) {
			var cmd = new FastString(CMD_INIT_BUFFER_SIZE);
			cmd.Append("SELECT ");
			if (isDistict) {
				cmd.Append("DISTINCT ");
			}

			if (isCount) cmd.Append("count(");
			if (columns == null || columns.Length < 1) {
				cmd.Append("*");
			}
			else {
				var first = true;

				foreach (var c in columns) {
					if (first) first = false; else cmd.Append(", ");
					cmd.Append(c);
				}
			}
			if (isCount) cmd.Append(")");

			cmd.Append(" FROM ").Append(tableName);

			if (!whereStr.IsEmpty()) {
				cmd.Append(" WHERE ").Append(whereStr);
			}

			if (!string.IsNullOrEmpty(groupBy)) {
				cmd.Append(" GROUP BY ").Append(groupBy);
			}

			if (!orderBy.IsEmpty()) {
				cmd.Append(" ORDER BY ").Append(orderBy.ToString());
			}

			if (limit > 0) {
				cmd.Append(" LIMIT ");
				if (limitOffset > 0) {
					cmd.Append(limitOffset).Append(",");
				}
				cmd.Append(limit);
			}

			cmd.Append(";");
			return cmd.ToString();
		}

		public SQuery.QueryResult Select(params string[] columns) {
			var cmd = CreateSelectQuery(false, columns);
			return db.ExecuteQuery(cmd, whereArgs.ToArray());
		}

		string[] GetColumnsFromCreator<T>(Func<T> creator) where T : ISQueryRow {
			return GetColumns_And_SetWhereForCheckKey(creator, false);
		}

		string[] GetColumns_And_SetWhereForCheckKey<T>(Func<T> creator, bool enbaleFillWhere = true)
			where T : ISQueryRow {
			var source = creator().ToValues();
			var cols = new string[source.Count];
			int i = 0;

			enbaleFillWhere = enbaleFillWhere && keys != null && keys.Count > 0 && whereStr.IsEmpty();
			FastString buffer = enbaleFillWhere ? new FastString(CMD_INIT_BUFFER_SIZE / 2) : null;
			List<object> argList = enbaleFillWhere ? new List<object>(source.Count) : null;

			bool isFirst = true;
			foreach (var kv in source) {
				cols[i] = kv.Key;

				if (enbaleFillWhere && keys.Contains(kv.Key)) {
					if (isFirst) isFirst = false; else buffer.Append(" AND ");

					buffer.Append(kv.Key).Append("=@kcArg" + i);
					argList.Add(kv.Value);
				}

				++i;
			}

			if (enbaleFillWhere) {
				Where(buffer.ToString(), argList.ToArray());
			}

			return cols;
		}

		void SetWhereForCheckKey(Dictionary<string, object> source) {
			bool enbaleFillWhere = keys != null && keys.Count > 0 && whereStr.IsEmpty();
			if (!enbaleFillWhere) return;

			FastString buffer = new FastString(CMD_INIT_BUFFER_SIZE / 2);
			List<object> argList = new List<object>(source.Count);
			int i = 0;

			bool isFirst = true;
			foreach (var kv in source) {
				if (keys.Contains(kv.Key)) {
					if (isFirst) isFirst = false; else buffer.Append(" AND ");

					buffer.Append(kv.Key).Append("=@kcArg" + i);
					argList.Add(kv.Value);
				}

				++i;
			}

			Where(buffer.ToString(), argList.ToArray());
		}

		public T SelectOne<T>(Func<T> creator) where T : ISQueryRow {
			var cols = GetColumns_And_SetWhereForCheckKey(creator);
			return SelectOne(creator, cols);
		}

		public T SelectOne<T>(Func<T> creator, params string[] columns)
			where T : ISQueryRow {
			Limit(1);
			using (var r = Select(columns)) {
				if (r.reader.HasRows && r.reader.Read()) {
					var newItem = creator();
					newItem.ParseFromDB(r.reader);
					Debug.Log("SelectOne: new Item: " + newItem);
					return newItem;
				}

				return default(T); // nullを返す
			}
		}

		public ICollection<T> Select<T>(Func<T> creator) where T : ISQueryRow {
			var cols = GetColumns_And_SetWhereForCheckKey(creator);
			return Select(creator, cols);
		}

		public ICollection<T> Select<T>(Func<T> creator, params string[] columns)
			where T : ISQueryRow {
			using (var r = Select(columns)) {
				var rows = new LinkedList<T>();
				while (r.reader.HasRows) {
					while (r.reader.Read()) {
						var newItem = creator();
						newItem.ParseFromDB(r.reader);
						rows.AddLast(newItem);
					}
					r.reader.NextResult();
				}

				return rows;
			}
		}

		public int Count(params string[] columns) {
			var cmd = CreateSelectQuery(true, columns);
			return Convert.ToInt32(db.ExecuteScalar(cmd));
		}

	}

}