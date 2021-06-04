<?php

namespace Ywnsyage\Clickhouse\Exceptions;

use GuzzleHttp\Exception\RequestException;
use Ywnsyage\Clickhouse\Query;
use Ywnsyage\Clickhouse\Server;

/**
 * @codeCoverageIgnore
 */
class TransportException extends \Exception
{
    public static function connectionError(Server $server, $reasonMessage)
    {
        return new static('Can\'t connect to the server ['.$server->getHost().':'.$server->getPort().'] with error: ['.$reasonMessage.']');
    }

    public static function serverReturnedError($exception, Query $query)
    {
        if ($exception instanceof RequestException) {
            $error = $exception->getResponse()->getBody()->getContents();
        } else {
            $error = $exception;
        }

        $errorString = 'Host ['.$query->getServer()->getHost().'] returned error: '.$error.'. Query: '.$query->getQuery();

        return new static($errorString);
    }

    public static function malformedResponseFromServer($response)
    {
        return new static('Malformed response from server: '.$response);
    }
}
