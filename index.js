const redis = require('redis')
const hash2json = require('hash-json')

class RedisConn
{
	constructor(cfg, prefix={prefix:'cache:'})
	{
        this.config_ = cfg;
        this.prefix_ = prefix.prefix;
	}

	connection(callback)
	{
		// use method: cli.quit() to disconnection.
		callback(redis.createClient(this.config_.port, this.config_.host));
    }
    
    getKey(key)
    {
        return `${this.prefix_}${key}`
    }

	dropTable(cli, tblname, callback)
	{
		cli.del(this.getKey(tblname));
		callback(cli);
	}

	// Hash
	addHash(cli, tblname, model, callback)
	{
		cli.hmset(this.getKey(tblname), hash2json.toHash(model), (err, res) => {
			callback(cli);
		});
	}
	
	reWriteHash(cli, tblname, model, callback)
	{
		this.dropTable(cli, tblname, (cli) => {
			this.addHash(cli, tblname, model, (cli) => {
				callback(cli);
			});
		});
	}

	getHash(cli, tblname, callback)
	{
		cli.hgetall(this.getKey(tblname), (err, res) => {
			callback(cli, res ? hash2json.toJson(res) : null);
		});
    }

    // Set
	getSet(cli, tblname, callback)
	{
		cli.smembers(this.getKey(tblname), (err, res) => {
			callback(cli, res ? res : []);
		});
	}

	addSet(cli, tblname, value, callback)
	{
		cli.sadd(this.getKey(tblname), value, (err, res) => {
			callback(cli);
		});
	}

	delSet(cli, tblname, value, callback)
	{
		cli.srem(this.getKey(tblname), value, (err, res) => {
			callback(cli);
		});
	}
}

module.exports = RedisConn;