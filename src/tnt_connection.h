#pragma once

#include <string>
#include <memory>

struct tnt_stream;
class Row;
namespace tnt {
	class Iterator;
	class TupleBuilder;
}

class TntConnection {
public:
	TntConnection();
	~TntConnection();

	void connect(const std::string &host_port);
	bool connected();

	std::shared_ptr<tnt::Iterator> select(const std::string &space, const tnt::TupleBuilder &builder);
	bool insert(const std::string &space, const tnt::TupleBuilder &builder);
	bool del(const std::string &space, const tnt::TupleBuilder &builder);
	bool replace(const std::string &space, const tnt::TupleBuilder &builder);

	int resolveSpace(const std::string &space);

	const std::string &lastError() const;
private:
	struct tnt_stream * tnt;
	std::string last_error;
	std::string host;
	int port;

	void shutdownConnection();
};

