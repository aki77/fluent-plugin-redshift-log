module Fluent
  class RedshiftLogOutput < Fluent::Output
    Fluent::Plugin.register_output('redshift_log', self)

    config_param :key_name, :string, :default => 'log'
    config_param :fields, :string

    include SetTagKeyMixin
    config_set_default :include_tag_key, false

    include SetTimeKeyMixin
    config_set_default :include_time_key, true
    config_set_default :time_format, "%Y-%m-%d %H:%M:%S"

    include Fluent::HandleTagNameMixin

    def initialize
      super
      require 'json'
    end

    def configure(conf)
      super

      if (
          !@remove_tag_prefix &&
          !@remove_tag_suffix &&
          !@add_tag_prefix    &&
          !@add_tag_suffix
      )
        raise Fluent::ConfigError, "At least one of remove_tag_prefix/remove_tag_suffix/add_tag_prefix/add_tag_suffix is required to be set"
      end

      @fields = @fields.split(',')
    end

    def emit(tag, es, chain)
      es.each {|time,record|
        new_tag = tag.clone
        new_record = record.clone

        filter_record(new_tag, time, new_record)
        Fluent::Engine.emit(new_tag, time, {@key_name => generate_log(new_record)})
      }

      chain.next
    end

    private
    def generate_log(record)
      val_list = @fields.collect do |field|
        val = record[field]
        val = nil unless (val || val.kind_of?(FalseClass)) && !val.to_s.empty?
        val = JSON.generate(val) if val.kind_of?(Hash) || val.kind_of?(Array)
        val = @timef.format(val) if val.kind_of?(Time)
        val.to_s unless val.nil?
      end

      generate_log_with_delimiter(val_list, "\t")
    end

    def generate_log_with_delimiter(val_list, delimiter)
      val_list = val_list.collect do |val|
        if val.nil? || val.empty?
          ""
        else
          val.gsub(/\\/, "\\\\\\").gsub(/\t/, "\\\t").gsub(/\n/, "\\\n") # escape tab, newline and backslash
        end
      end
      val_list.join(delimiter)
    end

  end
end
