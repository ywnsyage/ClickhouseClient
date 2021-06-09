<?php
/**
 * User：lost0719@163.com
 * Datetime：2021/6/4 09:52:14
 */


namespace Ywnsyage\Clickhouse\Transport;


use Ywnsyage\Clickhouse\Interfaces\TransportInterface;
use Ywnsyage\Clickhouse\Query;
use Ywnsyage\Clickhouse\Query\QueryStatistic;
use Ywnsyage\Clickhouse\Query\Result;
use Ywnsyage\Clickhouse\Common\TempTable;
use Ywnsyage\Clickhouse\Interfaces\FileInterface;

class MysqlTransport implements TransportInterface
{

    /**
     * @var MysqlClient
     */
    protected $mysqlClient;

    /**
     * HttpTransport constructor.
     *
     * @param MysqlClient $client
     * @param array  $options
     */
    public function __construct(MysqlClient $client = null, array $options = [])
    {
        $this->setClient($client);

        $this->options = $options;
    }

    /**
     * Returns flag to enable / disable queries and data compression.
     *
     * @return bool
     */
    protected function isDeflateEnabled(): bool
    {
        return $this->options['deflate'] ?? true;
    }

    /**
     * Sets Mysql client.
     *
     * @param MysqlClient|null $client
     */
    protected function setClient(Client $client = null)
    {
        if (is_null($client)) {
            $this->mysqlClient = $this->createMysqlClient();
        } else {
            $this->mysqlClient = $client;
        }
    }

    /**
     * Creates Guzzle client.
     */
    protected function createMysqlClient()
    {
        return new MysqlClient();
    }
    
    public function write(array $queries, int $concurrency = 5): array
    {
        // TODO: Implement write() method.
        $result = [];

        foreach ($queries as $index => $query) {
            /* @var Query $query */
            $server = $query->getServer();
            $res = $this->mysqlClient->build($server)->query($query->getQuery());
            $statistic = new QueryStatistic(
                $res['rows'] ?? 0,
                0,
                microtime(true),
                null
            );
            $result[$index] = new Result($query, $res['data'], $statistic);
        }

        return $result;
    }
    
    public function read(array $queries, int $concurrency = 5): array
    {
        // TODO: Implement read() method.
        $result = [];
        //print_r($queries);
        foreach ($queries as $index => $query) {
            /* @var Query $query */
            $server = $query->getServer();
            $res = $this->mysqlClient->build($server)->query($query->getQuery());
            
            $statistic = new QueryStatistic(
                $res['rows'] ?? 0,
                0,
                microtime(true),
                null
            );
            $result[$index] = new Result($query, $res['data'], $statistic);
            //var_dump($result[$index]);
        }
        return $result;
    }
}