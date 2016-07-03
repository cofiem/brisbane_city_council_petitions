class Petitions

  # Get petitions from list page
  # @param [String] page
  # @return [Hash]
  def get_petitions(page)
    html = Nokogiri::HTML(page)
    table = html.css('table[class="petitions"]')
    trs = table.css('tr')

    items = []

    if trs.size > 1
      trs.drop(1).each do |tr| # remove the header row
        item = parse_list_row(tr)
        items.push(item) unless item.nil?
      end
    end

    items
  end

  # Get petition from item page
  # @param [String] page
  # @return [Hash]
  def get_item(page)
    html = Nokogiri::HTML(page)
    table = html.css('div#content')

    {
        title: table.css('h1')[0].text.gsub(/\s+/, ' ').strip,
        body: table.css('div#petition-details')[0].text.gsub(/\s+/, ' ').strip,
        principal: table.css('table.petition-details tr td')[1].text.gsub(/\s+/, ' ').strip,
        closed_at: Time.zone.parse(table.css('table.petition-details tr td')[3].text.gsub(/\s+/, ' ').strip + ' 00:00:00 +10:00'),
        signatures: table.css('table.petition-details tr td')[5].text.split(' ').first.gsub(/\s+/, ' ').strip.to_i
    }
  end

  private

  def parse_list_row(html)
    tds = html.css('td')
    if tds.size < 1
      # no petitions, nothing to do here
      nil
    else
      uri = tds[0].css('a')[0]['href']
      ref_id = uri.split('/').last
      title = tds[0].text
      principal = tds[1].text
      closed_at = tds[2].text
      {
          reference_id: ref_id,
          title: title,
          principal: principal,
          closed_at: Time.zone.parse(closed_at + ' 00:00:00 +10:00')
      }
    end
  end
end