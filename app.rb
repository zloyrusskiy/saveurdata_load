require 'fileutils'
require 'pstore'
require 'rest-client'

URL = 'https://saverudata.net/db/dbpn/%s/%s/%s/%s.csv'
OUTPUT_DIR = File.join(File.dirname(__FILE__), 'output')
DB = PStore.new("loaded.pstore")

filepath = (0..99)
  .map { |part| "%.02d" % part }
  .repeated_combination(4)

unless Dir.exist?(File.join(OUTPUT_DIR, "00", "00", "00"))
  puts "creating directories"
  filepath.clone.each do |fp|
    dir_parts = fp[0..-2]
    file_dir = File.join(OUTPUT_DIR, *dir_parts)
    FileUtils.mkdir_p(file_dir)
  end
end

filepath.each do |fp|  
  DB.transaction do
    if DB[fp].nil?
      puts "loading #{fp}"

      begin
        resp = RestClient.get URL % fp
        if resp.code == 200
          File.write(File.join(file_dir, fp[-1]), resp.body)
        end
      rescue RestClient::NotFound => e
        puts "got rest client error #{e.inspect}"
      end

      DB[fp] = true
    end
  end
end
