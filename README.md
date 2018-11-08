# SQuery for Unity
Simple SQLite Query Library for Unity (5.6.5 later)

* 利用したライブラリ
  * SQLite3 https://www.sqlite.org/index.html
  * FastString: https://github.com/snozbot/FastString
    * StringBuilder クラスで代用可能

# Select
```c#
string dbPath = Defines.Path.MASTER_DATA + "master.dat";

public class PatternDBItem : ISQueryRow {
	public int pattern_id;
	int seq;
	public string title_jp;
	public string title_en;
	public string text_jp;
	public string text_en;
	public string img;
	public string snd_jp;
	public string snd_en;
	
	public void ParseFromDB(SqliteDataReader reader) {
		int idx = reader.GetOrdinal("pattern_id");
		pattern_id = reader.GetInt32(idx);

		idx = reader.GetOrdinal("seq");
		if (idx >= 0) seq = reader.GetInt32(idx);

		title_jp = reader["title_jp"].ToString();
		title_en = reader["title_en"].ToString();

		// 省略
	}

	public Dictionary<string, object> ToValues() {
		return new Dictionary<string, object> {
			{"pattern_id", pattern_id},
			{"seq", seq},
			{"title_jp", title_jp},
			{"title_en", title_en},
			{"text_jp", text_jp},
			{"text_en", text_en},
			{"img", img},
			{"snd_jp", snd_jp},
			{"snd_en", snd_en}
		};
	}
}

// SELECT * FROM patterns WHERE pattern_id=@id ORDER BY seq
public ICollection<PatternDBItem> GetPatterns(int pattern_id) {
    return new SQuery(dbPath)
        .From("patterns")
        .Where("pattern_id=@id", pattern_id)
        .OrderBy("seq")
        .Select(() => new PatternDBItem());
}

// SELECT DISTNICT pattern_id,title_jp,title_en FROM patterns
// WHERE pattern_id >= @begin AND pattern_id <= @end
// ORDER BY pattern_id
// GROUP BY pattern_id
public ICollection<PatternDBItem> GetPatternGroups() {
    return new SQuery(dbPath)
        .From("patterns")
        .Where("pattern_id >= @begin AND pattern_id <= @end", 1, 100)
        .OrderBy("pattern_id")
        .GroupBy("pattern_id")
        .Distnict()
        .Select(() => new PatternDBItem(), "pattern_id", "title_jp", "title_en");
}

// SELECT count(pattern_id) FROM pattern WHERE count >= 1
var count = new SQuery(USER_DB)
    .From("pattern")
    .Where("count >= 1")
    .Count("pattern_id");
```

# Insert or Update
```c#
// INSERT INTO pattern (pattern_id,count,update_datetime) VALUES (id, cnt, now);
//
// INSERTが失敗したら
// UPDATE pattern SET pattern_id=@id, count=@cnt, update_datetime=@now 
// WHERE pattern_id = @id AND count < @cnt;
public static void ImportPatternRecords(SimpleJSON.JSONArray json) {
	var tbl = new SQuery(USER_DB).From("pattern");
	var nowRaw = DateTime.UtcNow;
	var now = SQuery.DateTimeToStr(nowRaw);

	for (int i = 0; i < json.Count; i++) {
		var node = json[i];
		var id = node["no"].AsInt;
		var cnt = node["cnt"].AsInt;
		var values = new Dictionary<string, object> {
			{ "pattern_id", id},
			{ "count", cnt },
			{ "update_datetime", now }
		};
		tbl.Keys("pattern_id")
		   .Values(values)
		   .Where("pattern_id = @id AND count < @cnt", id, cnt)
		   .InsertOrUpdate();
	}
}
```
`Keys()`でキーを指定すると、Updateの時にSETからキーのフィルドは外される。

# Delete
```c#
// DELETE FROM user_img WHERE update_datetime < @date
new SQuery(USER_DB)
    .From("user_img")
    .Where("update_datetime < @date", "2000-01-01", phraseNo)
    .Delete();
```


