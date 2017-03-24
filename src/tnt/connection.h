#pragma once

#include <string>
#include <memory>
#include <map>

struct tnt_stream;
class Row;
namespace tnt {
	class Iterator;
	class TupleBuilder;

class Connection {
public:
	Connection();
	~Connection();

	void connect(const std::string &host_port);
	bool connected();

	std::shared_ptr<tnt::Iterator> select(const std::string &space, const tnt::TupleBuilder &builder);
	std::shared_ptr<tnt::Iterator> select(int space_id, const tnt::TupleBuilder &builder);

	bool insert(const std::string &space, const tnt::TupleBuilder &builder);
	bool insert(int space_id, const tnt::TupleBuilder &builder);

	bool del(const std::string &space, const tnt::TupleBuilder &builder);
	bool del(int space_id, const tnt::TupleBuilder &builder);

	bool replace(const std::string &space, const tnt::TupleBuilder &builder);
	bool replace(int space_id, const tnt::TupleBuilder &builder);

	int resolveSpace(const std::string &space);

	const std::string &lastError() const;
private:
	struct tnt_stream * tnt;
	std::string last_error;
	std::string host;
	std::map<std::string, int> spaces;
	int port;

	void shutdownConnection();
	static void deleteStream(struct tnt_stream *stream);
};

}
