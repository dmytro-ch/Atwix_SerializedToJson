<?php
/**
 * @author Atwix Team
 * @copyright Copyright (c) 2017 Atwix (https://www.atwix.com/)
 * @package Atwix_SerializedToJson
 */

namespace Atwix\SerializedToJson\Console\Command;

use Exception;
use Magento\Framework\App\ResourceConnection;
use Magento\Framework\Db\Select;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Magento\Framework\Console\Cli;

/**
 * Class ConvertEmptyValuesCommand
 */
class ConvertEmptyValuesCommand extends Command
{
    /**
     * Argument key for table name
     */
    const ARGUMENT_TABLE = 'table';

    /**
     * Argument key for identifier field name
     */
    const ARGUMENT_ID_FIELD = 'id-field';

    /**
     * Argument key for field name to be processed
     */
    const ARGUMENT_FIELD = 'field';

    /**
     * Resource Connection
     *
     * @var ResourceConnection
     */
    protected $resource;

    /**
     * ConvertEmptyValuesCommand constructor
     *
     * @param ResourceConnection $resource
     * @param null $name
     */
    public function __construct(ResourceConnection $resource, $name = null)
    {
        parent::__construct($name);
        $this->resource = $resource;
    }

    /**
     * {@inheritdoc}
     */
    protected function configure()
    {
        $this->setName('atwix:serialized-to-json:empty-values-fix');
        $this->setDescription('Convert empty string values to valid serialized object');
        $this->addArgument(self::ARGUMENT_TABLE, InputArgument::REQUIRED, 'Table name');
        $this->addArgument(self::ARGUMENT_ID_FIELD, InputArgument::REQUIRED, 'Identifier field name');
        $this->addArgument(self::ARGUMENT_FIELD, InputArgument::REQUIRED, 'Field to be validated');
    }

    /**
     * {@inheritdoc}
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $tableName = $input->getArgument(self::ARGUMENT_TABLE);
        $idFieldName = $input->getArgument(self::ARGUMENT_ID_FIELD);
        $fieldName = $input->getArgument(self::ARGUMENT_FIELD);
        // Generate a valid serialised empty object as string
        $emptySerializedValue = serialize('');
        $connection = $this->resource->getConnection();

        /** @var Select $select */
        $select = $connection->select();
        $select->from($tableName);
        $select->where(sprintf('%s = \'\'', $fieldName));
        $rows = $connection->fetchAll($select);
        $count = count($rows);

        if (!$count) {
            $output->writeln(
                sprintf('The "%s" field does not contain empty values in "%s" table', $fieldName, $tableName)
            );

            return 0;
        }

        $output->writeln(sprintf('Empty values count: %s', $count));
        $connection->beginTransaction();

        try {
            foreach ($rows as $row) {
                // Replace empty value
                $row[$fieldName] = $emptySerializedValue;
                $where = [$idFieldName . ' = ?' => $row[$idFieldName]];
                $connection->update($tableName, $row, $where);
            }
            $connection->commit();
        } catch (Exception $e) {
            $connection->rollBack();
            $output->writeln("<error>{$e->getMessage()}</error>");

            return Cli::RETURN_FAILURE;
        }

        $output->writeln(sprintf('Successfully replaced empty values'));

        return 0;
    }
}