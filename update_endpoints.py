import csv
import requests
import pandas as pd

from bs4 import BeautifulSoup

netowrks =['eth', 'arb']
chain_number=1

def scrape_rpc_endpoints_eth1():
    url = "https://ethereumnodes.com/"
    response = requests.get(url)

    soup = BeautifulSoup(response.text, 'html.parser')
    
    rpc_endpoints = []

    elements = soup.find_all('li', class_=lambda x: x and x.startswith('jsx-'))
    for element in elements:
        status_element = element.find('span', class_=lambda x: x and 'status' in x and 'up' in x)
        if status_element:
            endpoint_element = element.find('input', class_=lambda x: x and x.startswith('jsx-') and 'endpoint' in x)
            if endpoint_element:
                endpoint = endpoint_element.get('value')
                rpc_endpoints.append(endpoint)

    print(rpc_endpoints)
    save_to_csv(rpc_endpoints, f'rpc_endpoints_eth1.csv')

def save_to_csv(data, filename):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows([[endpoint] for endpoint in data])


def scrape_chain_data(chain_number):
    url = f"https://chainlist.org/chain/{chain_number}"
    
    # Add a user-agent header to the request
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36'
    }
    
    # Send a GET request to the URL with the headers
    response = requests.get(url, headers=headers)

    # Create BeautifulSoup object to parse the HTML content
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find the table containing the chain data
    table = soup.find('table', class_='m-0')


    if not table:
        print(f"Table not found for chain {chain_number}")
        return

    # Create a CSV file to write the data
    filename = f"chain_data_{chain_number}.csv"
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)

        # Write the header row
        writer.writerow(["RPC Server Address"])

        # Find all table rows except the header row
        rows = table.find_all('tr')[1:]

        # Iterate through the rows and extract the RPC Server Address where the score column has fill="green"
        for row in rows:
            columns = row.find_all('td')

            print(row)


            # Ensure that the row has the expected number of columns
            if len(columns) >= 5:
                score_column = columns[4]

                print(columns[3].text.strip())
                print('')
                # Check if the score column has fill="green"
                if score_column.find('svg', {'fill': 'green'}):
                    rpc_server_address = columns[3].text.strip()
                    print(rpc_server_address)


                    # Write the RPC Server Address to the CSV file
                    writer.writerow([rpc_server_address])

    print("Scraping completed. Data saved in", filename)


# Example usage:
scrape_chain_data(chain_number)