"use client";
import { useState, useEffect, Children, isValidElement, useRef } from "react";
import { CodeGroup } from "./CodeGroup";
import { Button } from "../atoms/Button";
import React from "react";
import clsx from "clsx";

interface TabbedCodeGroupProps {
  children: React.ReactNode;
  default?: string;
  savedPreferenceKey?: string;
  className?: string;
}

export const TAB_CHANGE_EVENT = "tabbedCodeGroupChange";
export function TabbedCodeGroup({
  children,
  default: defaultTab,
  savedPreferenceKey,
  ...props
}: TabbedCodeGroupProps) {
  const [activeTab, setActiveTab] = useState("");
  const hasSetInitialTab = useRef(false);

  const tabItems = Children.toArray(children).filter(
    (child): child is React.ReactElement<TabbedCodeGroupItemProps> => {
      return isValidElement(child);
    },
  );

  const tabValues = tabItems.map(
    (item) => item.props.value || item.props.label,
  );

  useEffect(() => {
    const handleSelectionChange = (event: CustomEvent) => {
      if (event.detail.key === savedPreferenceKey) {
        const newTab = event.detail.value;
        if (newTab && tabValues.includes(newTab)) {
          setActiveTab(newTab);
        }
      }
    };

    if (savedPreferenceKey) {
      window.addEventListener(
        TAB_CHANGE_EVENT as any, // Yes, ugly type hack, because otherwise I need to create a new definition for window
        handleSelectionChange,
      );
      const preferredTab = window.localStorage.getItem(
        "_tcgpref_" + savedPreferenceKey,
      );
      if (preferredTab && tabValues.includes(preferredTab)) {
        setActiveTab(preferredTab);
        hasSetInitialTab.current = true;
      }
    }

    if (!hasSetInitialTab.current && tabValues.length > 0) {
      const initialTab =
        defaultTab && tabValues.includes(defaultTab)
          ? defaultTab
          : tabValues[0];
      setActiveTab(initialTab);
      hasSetInitialTab.current = true;
    }

    return () => {
      if (savedPreferenceKey) {
        window.removeEventListener(
          TAB_CHANGE_EVENT as any, // See above
          handleSelectionChange,
        );
      }
    };
  }, [defaultTab, savedPreferenceKey, tabValues.length]);

  const activeTabIndex = tabValues.indexOf(activeTab);
  const activeContent = tabItems[activeTabIndex];

  if (tabValues.length === 0) {
    return <div>{children}</div>;
  }

  return (
    <div className={clsx("flex flex-col gap-4", props.className)} {...props}>
      <div className="flex gap-1 border-b border-stone-200 dark:border-stone-700">
        {tabItems.map((item, index) => {
          const itemValue = item.props.value || item.props.label;
          return (
            <Button
              key={itemValue}
              variant="ghost"
              size="sm"
              className={`
                rounded-b-none border-b-2 transition-colors
                ${
                  activeTab === itemValue
                    ? "border-primary bg-primary/5 text-primary"
                    : "border-transparent hover:border-stone-300 dark:hover:border-stone-600"
                }
              `}
              onClick={() => {
                if (savedPreferenceKey) {
                  window.localStorage.setItem(
                    "_tcgpref_" + savedPreferenceKey,
                    itemValue,
                  );
                  window.dispatchEvent(
                    new CustomEvent(TAB_CHANGE_EVENT, {
                      detail: {
                        key: savedPreferenceKey,
                        value: itemValue,
                      },
                    }),
                  );
                }
                setActiveTab(itemValue);
              }}
            >
              {item.props.icon && (
                <span
                  className={`flex items-center ${
                    activeTab === itemValue ? "" : "grayscale opacity-60"
                  }`}
                >
                  {item.props.icon}
                </span>
              )}
              {item.props.label}
            </Button>
          );
        })}
      </div>
      <div>
        {activeContent ? (
          React.cloneElement(activeContent as React.ReactElement)
        ) : (
          <div className="text-stone-500 dark:text-stone-400 p-4 text-center">
            No content available for tab: {activeTab}
          </div>
        )}
      </div>
    </div>
  );
}

TabbedCodeGroup.displayName = "TabbedCodeGroup";

interface TabbedCodeGroupItemProps {
  children: React.ReactNode;
  label: string;
  value?: string;
  className?: string;
  icon?: React.ReactNode;
}

export function TabbedCodeGroupItem({
  children,
  className,
  ...props
}: TabbedCodeGroupItemProps) {
  return (
    <CodeGroup className={className} {...props}>
      {children}
    </CodeGroup>
  );
}

TabbedCodeGroupItem.displayName = "TabbedCodeGroupItem";
