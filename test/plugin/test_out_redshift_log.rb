require 'helper'

class RedshiftLogOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
    add_tag_prefix redshift.
    fields         hoge,fuga
   ]
  # CONFIG = %[
  #   path #{TMP_DIR}/out_file_test
  #   compress gz
  #   utc
  # ]

  def create_driver(conf = CONFIG, tag='test')
    Fluent::Test::OutputTestDriver.new(Fluent::RedshiftLogOutput, tag).configure(conf)
  end

  def test_configure
    #### set configurations
    d = create_driver %[
      add_tag_prefix redshift.
      fields         hoge,fuga
    ]

    #### check configurations
    assert_equal ['hoge', 'fuga'], d.instance.fields
    assert_equal true, d.instance.include_time_key
  end

  def test_format
    d = create_driver

    # time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    # d.emit({"a"=>1}, time)
    # d.emit({"a"=>2}, time)

    # d.expect_format %[2011-01-02T13:14:15Z\ttest\t{"a":1}\n]
    # d.expect_format %[2011-01-02T13:14:15Z\ttest\t{"a":2}\n]

    # d.run
  end

  def test_emit_default
    # test1 default config
    driver = create_driver

    driver.run do
      driver.emit({ 'hoge' => 'bar', 'fuga' => 'foo' })
      driver.emit({ 'fuga' => 74, 'hoge' => true })
      driver.emit({ 'fuga' => 74, 'hoge' => "foo\tfoo" })
    end
    emits = driver.emits

    assert_equal 3, emits.count

    # ["redshift.test", 1354689632, {"log"=>"bar\tfoo"}]
    assert_equal     'redshift.test', emits[0][0]
    assert_equal          "bar\tfoo", emits[0][2]['log']

    # ["redshift.test", 1354689632, {"log"=>"true\t74"}]
    assert_equal     'redshift.test', emits[1][0]
    assert_equal          "true\t74", emits[1][2]['log']

    # ["redshift.test", 1354689632, {"log"=>"foo\\\tfoo\t74"}]
    assert_equal     'redshift.test', emits[2][0]
    assert_equal    "foo\\\tfoo\t74", emits[2][2]['log']
  end

  def test_emit_include_time
    driver = create_driver(%[
      add_tag_prefix redshift.
      fields         time,hoge,fuga
    ])

    time = Time.parse("2013-06-12T00:00:00+09:00")

    driver.run do
      driver.emit({ 'hoge' => 'bar', 'fuga' => 'foo' }, time)
    end
    emits = driver.emits

    assert_equal 1, emits.count

    # ["redshift.test", 1370962800, {"log"=>"2013-06-11 15:00:00\tbar\tfoo"}]
    assert_equal                 'redshift.test', emits[0][0]
    assert_equal "2013-06-11 15:00:00\tbar\tfoo", emits[0][2]['log']
  end

  def test_emit_time_field
    driver = create_driver(%[
      add_tag_prefix redshift.
      fields         time,hoge,created_at
    ])

    time = Time.parse("2013-06-12T00:00:00+09:00")
    created_at = Time.parse("2013-06-11T00:00:00+09:00")

    driver.run do
      driver.emit({ 'created_at' => created_at, 'hoge' => 'bar', 'fuga' => 'foo' }, time)
    end
    emits = driver.emits

    assert_equal 1, emits.count

    # ["redshift.test", 1370962800, {"log"=>"2013-06-11 15:00:00\tbar\t2013-06-10 15:00:00"}]
    assert_equal                                 'redshift.test', emits[0][0]
    assert_equal "2013-06-11 15:00:00\tbar\t2013-06-10 15:00:00", emits[0][2]['log']
  end

  def test_emit_empty_field
    driver = create_driver(%[
      add_tag_prefix redshift.
      fields         dummy1,hoge,dummy2,fuga
    ])

    driver.run do
      driver.emit({ 'hoge' => 'bar', 'fuga' => 'foo', 'dummy2' => nil })
    end
    emits = driver.emits

    assert_equal 1, emits.count

    # ["redshift.test", 1372391656, {"log"=>"\tbar\t\tfoo"}]
    assert_equal  'redshift.test', emits[0][0]
    assert_equal   "\tbar\t\tfoo", emits[0][2]['log']
  end

  def test_emit_boolean_field
    driver = create_driver(%[
      add_tag_prefix redshift.
      fields         field1,field2,field3
    ])

    driver.run do
      driver.emit({ 'field1' => 'value1', 'field2' => true, 'field3' => false })
    end
    emits = driver.emits

    assert_equal 1, emits.count

    # ["redshift.test", 1372391656, {"log"=>"value1\ttrue\tfalse"}]]
    assert_equal       'redshift.test', emits[0][0]
    assert_equal "value1\ttrue\tfalse", emits[0][2]['log']
  end

end
