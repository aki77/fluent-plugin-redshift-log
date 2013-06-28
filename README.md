# Fluent::Plugin::Redshift::Log

## Synopsis

Imagin you have a config as below:

```
<match access.**>
  type           redshift_log
  add_tag_prefix redshift.
  fields         time,uid,ua,uri
</match>

<match redshift.access.**>
  type       redshift
  file_type  tsv
….
</match>
```

And you feed such a value into fluentd:

```
"access" => {
  "uid" => 1234,
  "uri" => "/home",
  "ua"  => "iPhone",
}
```

Then you'll get re-emmited tag/record-s below:

```
"redshift.access" => { "log" => "2013-06-28 06:00:00\t1234\tiPhone\t/home" }
```