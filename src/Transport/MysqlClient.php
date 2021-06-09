<?php
/**
 * User：lost0719@163.com
 * Datetime：2021/6/4 10:09:25
 */


namespace Ywnsyage\Clickhouse\Transport;


use Ywnsyage\Clickhouse\Exceptions\TransportException;
use Ywnsyage\Clickhouse\Server;

class MysqlClient
{
    /** @var \mysqli */
    private $conn;

    /**
     * @param Server $server
     * @return $this
     * @throws TransportException
     */
    public function build(Server $server)
    {
        $host = $server->getHost();
        $port = $server->getPort();
        $database = $server->getDatabase();
        $username = $server->getUsername();
        $password = $server->getPassword();
        $this->conn = mysqli_connect($host, $username, $password, $database, $port);
        if(!$this->conn){
            throw TransportException::connectionError($server, 'clickHouse连接失败:'.mysqli_connect_error());
        }
        return $this;
    }

    /**
     * @param $sql
     * @return array|bool
     */
    public function query($sql)
    {
        //print_r($sql);
        $result = [
            'data' => [],
            'rows' => 0
        ];
        $res =  $this->conn->query($sql);
        if($res){
            $result['rows'] = $this->conn->affected_rows;
            $result['data'] = [$res];
        }
        if($res instanceof \mysqli_result){
            $result['data'] = mysqli_fetch_all($res, MYSQLI_ASSOC);
        }
        
        $this->conn->close();
        return $result;
    }


}