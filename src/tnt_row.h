#pragma once
#include <cstdint>
#include <cstddef>
#include <string>

class TntRow
{
	TntRow(const TntRow &) = delete;
	TntRow& operator = (const TntRow &) = delete;
public:
	TntRow(const char *data, size_t sz);
	~TntRow();

	int getInt();
	std::string getString();
private:
	std::size_t size;
	char *data;
	const char *p;
};
