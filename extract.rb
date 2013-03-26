require 'open-uri'
require 'nokogiri'
require 'microdata'
require 'rubberband'
require 'springboard'
require 'pry'

sitemap_index = open(ARGV[0]).read

es_client = ElasticSearch.new('http://localhost:9200', :index => "resources", :type => "resource")

beginning_time = Time.now
resources_count = 0
indexed_resources = 0

Nokogiri::HTML(sitemap_index).xpath('//loc').each do |sitemap_loc|
  puts sitemap_loc.text
  sitemap = open(sitemap_loc)
  begin
    sitemap = Zlib::GzipReader.new( sitemap ).read
  rescue => e
    sitemap = sitemap.read
  end
  Nokogiri::HTML(sitemap).xpath('//loc').each do |loc|
    url = loc.content
    open(url) do |f|
      items = Microdata::Document.new(f, url).extract_items
      items.each do |item|
        item_hash = item.to_hash
        types = item_hash[:type].map{|itemtype| itemtype.sub('http://schema.org/', '') }.join('AND')
        item_hash[:id] = types + '--' + url
        if item_hash[:properties]
          item_hash[:itemprops] = item_hash[:properties].keys 
        end
        puts JSON.pretty_generate(item_hash)
        begin # sometimes the data isn't to ES's liking--like values that are empty strings
          es_client.index(item_hash.merge(:url => url)) 
          indexed_resources += 1
        rescue
        end       
      end
    end
    resources_count += 0
  end
end

puts "Total resources: #{resources_count}"
puts "Indexed resources: #{indexed_resources}"
puts "Time elapsed #{Time.now - beginning_time} seconds"