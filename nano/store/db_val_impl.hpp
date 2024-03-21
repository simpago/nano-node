#pragma once

#include <nano/lib/blocks.hpp>
#include <nano/secure/account_info.hpp>
#include <nano/secure/pending_info.hpp>
#include <nano/store/db_val.hpp>

template <typename T>
nano::store::db_val<T>::db_val (std::shared_ptr<nano::block> const & val_a) :
	buffer (std::make_shared<std::vector<uint8_t>> ())
{
	{
		nano::vectorstream stream (*buffer);
		nano::serialize_block (stream, *val_a);
	}
	convert_buffer_to_value ();
}

template <typename T>
nano::store::db_val<T>::db_val (nano::pending_info const & val_a) :
	db_val (val_a.db_size (), const_cast<nano::pending_info *> (&val_a))
{
	static_assert (std::is_standard_layout<nano::pending_info>::value, "Standard layout is required");
}

template <typename T>
nano::store::db_val<T>::db_val (nano::pending_key const & val_a) :
	db_val (sizeof (val_a), const_cast<nano::pending_key *> (&val_a))
{
	static_assert (std::is_standard_layout<nano::pending_key>::value, "Standard layout is required");
}

template <typename T>
nano::store::db_val<T>::operator nano::account_info () const
{
	nano::bufferstream stream (reinterpret_cast<uint8_t const *> (data ()), size ());
	nano::account_info result;
	debug_assert (size () == result.db_size ());
	bool error = result.deserialize (stream);
	debug_assert (!error);
	return result;
}

template <typename T>
nano::store::db_val<T>::operator nano::pending_info () const
{
	nano::pending_info result;
	debug_assert (size () == result.db_size ());
	std::copy (reinterpret_cast<uint8_t const *> (data ()), reinterpret_cast<uint8_t const *> (data ()) + result.db_size (), reinterpret_cast<uint8_t *> (&result));
	return result;
}

template <typename T>
nano::store::db_val<T>::operator nano::pending_key () const
{
	nano::pending_key result;
	debug_assert (size () == sizeof (result));
	static_assert (sizeof (nano::pending_key::account) + sizeof (nano::pending_key::hash) == sizeof (result), "Packed class");
	std::copy (reinterpret_cast<uint8_t const *> (data ()), reinterpret_cast<uint8_t const *> (data ()) + sizeof (result), reinterpret_cast<uint8_t *> (&result));
	return result;
}
