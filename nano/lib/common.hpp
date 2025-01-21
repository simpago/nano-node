#pragma once

namespace boost::asio::ip
{
class address;
class tcp;
template <typename InternetProtocol>
class basic_endpoint;
}

namespace nano
{
using ip_address = boost::asio::ip::address;
using endpoint = boost::asio::ip::basic_endpoint<boost::asio::ip::tcp>;
using tcp_endpoint = endpoint;
}
