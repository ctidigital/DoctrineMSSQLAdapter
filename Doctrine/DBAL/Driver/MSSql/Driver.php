<?php

 /**
 * CTI Digital
 *
 * @author Jason Brown <j.brown@ctidigital.com>
 */

namespace Application\Database\Doctrine\DBAL\Driver\MSSql;

use Application\Database\Doctrine\DBAL\Driver\MSSql\Connection as DriverConnection;
use Doctrine\DBAL\Connection;

/**
 * Class Driver
 * @package Application\Database\Doctrine\DBAL\MSSql
 */
class Driver implements \Doctrine\DBAL\Driver
{

    /**
     * Attempts to create a connection with the database.
     *
     * @param array $params        All connection parameters passed by the user.
     * @param string|null $username      The username to use when connecting.
     * @param string|null $password      The password to use when connecting.
     * @param array $driverOptions The driver options to use when connecting.
     *
     * @return \Doctrine\DBAL\Driver\Connection The database connection.
     */
    public function connect(array $params, $username = null, $password = null, array $driverOptions = array())
    {
        if (!isset($params['host'])) {
            //throw new SQLSrvException("Missing 'host' in configuration for sqlsrv driver.");
        }
        if (!isset($params['dbname'])) {
            //throw new SQLSrvException("Missing 'dbname' in configuration for sqlsrv driver.");
        }

        $serverName = $params['host'];
        if (isset($params['port'])) {
            $serverName .= ':' . $params['port'];
        }
        $driverOptions['database'] = $params['dbname'];
        $driverOptions['username'] = $username;
        $driverOptions['password'] = $password;

        return new DriverConnection($serverName, $driverOptions);
    }

    /**
     * Gets the DatabasePlatform instance that provides all the metadata about
     * the platform this driver connects to.
     *
     * @return \Doctrine\DBAL\Platforms\AbstractPlatform The database platform.
     */
    public function getDatabasePlatform()
    {
        return new \Doctrine\DBAL\Platforms\SQLServer2008Platform();
    }

    /**
     * Gets the SchemaManager that can be used to inspect and change the underlying
     * database schema of the platform this driver connects to.
     *
     * @param \Doctrine\DBAL\Connection $conn
     *
     * @return \Doctrine\DBAL\Schema\AbstractSchemaManager
     */
    public function getSchemaManager(Connection $conn)
    {
        return new \Doctrine\DBAL\Schema\SQLServerSchemaManager($conn);
    }

    /**
     * Gets the name of the driver.
     *
     * @return string The name of the driver.
     */
    public function getName()
    {
        return 'mssql';
    }

    /**
     * Gets the name of the database connected to for this driver.
     *
     * @param \Doctrine\DBAL\Connection $conn
     *
     * @return string The name of the database.
     */
    public function getDatabase(Connection $conn)
    {
        $params = $conn->getParams();
        return $params['dbname'];
    }
}