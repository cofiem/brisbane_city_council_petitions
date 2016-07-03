require 'scraperwiki'
require 'mechanize'
require 'nokogiri'
require 'active_support'
require 'active_support/core_ext'
require './petitions'

Time.zone = 'Brisbane'

URI_LIST = 'http://www.epetitions.brisbane.qld.gov.au/'
URI_ITEM = 'http://www.epetitions.brisbane.qld.gov.au/petition/view/pid/%{petition_num}'
URI_ITEM_SIGN = 'http://www.epetitions.brisbane.qld.gov.au/petition/sign/pid/%{petition_num}'

petitions_helper = Petitions.new
current_time = Time.zone.now

# Get and save petitions
petitions_hash = []
open(URI_LIST) do |i|
  petitions_page = i.read
  petitions_hash = petitions_helper.get_petitions(petitions_page)
end

petitions_hash.each do |petition|
  uri = URI_ITEM % {petition_num: petition[:reference_id]}
  new_hash = {
      retrieved_at: current_time,
      url: uri,
      sign_uri: URI_ITEM_SIGN % {petition_num: petition[:reference_id]}
  }

  open(uri) do |i|
    petition_page = i.read
    petition_hash = petitions_helper.get_item(petition_page)

    new_hash.merge!(petition).merge!(petition_hash)

  end

  ScraperWiki.save_sqlite([:reference_id, :signatures], new_hash, 'data')
end
