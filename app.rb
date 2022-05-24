require 'fileutils'
require 'parallel'
require 'pg'
require 'rest-client'
require 'csv'
require 'json'
require 'sequel'

URL = 'https://saverudata.net/db/dbpn/%s/%s/%s/%s.csv'
DB_NAME = 'people_data'
DB = Sequel.connect(
  "postgres://#{ENV['POSTGRES_USER']}:#{ENV['POSTGRES_PASSWORD']}@localhost:5432/#{DB_NAME}",
  max_connections: 75,
  sslmode: 'disable',
  logger: Logger.new('logs/db.log', level: Logger::ERROR))

filepath = (0..99)
             .map { |part| "%.02d" % part }
             .repeated_permutation(3)

(0..99)
  .sort_by { |n|
    if n == 79
      -1
    else
      n
    end }
  .map { |part| "%.02d" % part }
  .each do |left|
  Parallel.each(filepath, in_threads: 8) do |fp|
    full_fp = [left] + fp
    phone_prefix = full_fp.join
    already_loaded = DB[:saveurdata_load_status].where(phone_prefix: phone_prefix).first

    unless already_loaded
      puts "loading #{full_fp}"

      http_status = nil

      begin
        resp = RestClient.get URL % full_fp
        http_status = resp.code
        if resp.code == 200
          data_as_hashes = CSV.parse(resp.body.force_encoding("utf-8"), headers: true).map(&:to_h)
          data_for_db = data_as_hashes.map { |d| [d["phone_number"], JSON.generate(d)] }

          DB[:saveurdata].import([:phone, :data], data_for_db)
        end
      rescue RestClient::NotFound => e
        http_status = 404
        puts "got rest client error #{e.inspect} for #{full_fp}"
      rescue StandardError, RuntimeError => e
        puts "got error #{e.inspect}, waiting and retrying"
        sleep 5 * 60 # 5 minutes
        retry
      end

      DB[:saveurdata_load_status].insert(phone_prefix: phone_prefix, http_status: http_status)
    end
  end
end
